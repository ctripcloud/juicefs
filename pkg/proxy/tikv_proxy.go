package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	meta "github.com/juicedata/juicefs/pkg/meta"
	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
	"github.com/juicedata/juicefs/pkg/utils"
	plog "github.com/pingcap/log"
	"github.com/sirupsen/logrus"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	zap "go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var batchSize = meta.DirBatchNum["kv"] + 1

const (
	tikvProxySessionKeyPrefix    = "__tikv_proxy_session__/"
	tikvProxySessionLastCleanKey = "__tikv_proxy_session_last_clean__"
)

var (
	tikvProxySessionHeartbeatInterval = time.Second * 3
	tikvProxySessionHeartbeatTimeout  = time.Second * 10
)

// TiKVProxy implements proxy for tikv with transaction management
type TiKVProxy struct {
	client    *txnkv.Client
	cancel    context.CancelFunc
	clusterID uint64
	closeOnce sync.Once
	closed    bool
	mu        sync.RWMutex
	proxyv1.UnimplementedTxnProxyServiceServer
}

var logger = utils.GetLogger("juicefs")

func NewTiKVProxy(addr string, proxyAddr string) (*TiKVProxy, error) {
	logger.Infof("NewTiKVProxy, addr: %s, proxyAddr: %s", addr, proxyAddr)
	// default timeout is 1 second, it is dangerous for a large number of tikv clients
	// please check the issue: https://git.dev.sh.ctripcorp.com/dre/issues/-/issues/1008
	tikv.SetStoreLivenessTimeout(time.Second * 5)

	var plvl string // TiKV (PingCap) uses uber-zap logging, make it less verbose
	switch logger.Level {
	case logrus.TraceLevel:
		plvl = "debug"
	case logrus.DebugLevel:
		plvl = "info"
	case logrus.InfoLevel, logrus.WarnLevel:
		plvl = "warn"
	case logrus.ErrorLevel:
		plvl = "error"
	default:
		plvl = "dpanic"
	}
	l, prop, _ := plog.InitLogger(&plog.Config{Level: plvl}, zap.Fields(zap.String("component", "tikv"), zap.Int("pid", os.Getpid())))
	plog.ReplaceGlobals(l, prop)
	tUrl, err := url.Parse("tikv://" + addr)
	if err != nil {
		return nil, err
	}
	query := tUrl.Query()
	if query != nil && query.Has("ca") && query.Has("cert") && query.Has("key") {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Security = config.NewSecurity(
				query.Get("ca"),
				query.Get("cert"),
				query.Get("key"),
				strings.Split(query.Get("verify-cn"), ","))
		})
	} else {
		logger.Infoln("TiKV use default security config")
		// create tls files
		ca, client_crt, client_key, err := utils.CreateCertFile([]byte(meta.GetTIKVCaTlsData()), []byte(meta.GetTIKVClientCertTlsData()), []byte(meta.GetTIKVClientKeyTlsData()))
		if err != nil {
			return nil, err
		}
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Security = config.NewSecurity(
				ca,
				client_crt,
				client_key,
				[]string{})
		})
	}

	client, err := txnkv.NewClient(strings.Split(addr, ","))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	proxy := &TiKVProxy{
		client:    client,
		cancel:    cancel,
		clusterID: client.GetClusterID(),
	}
	if proxyAddr != "" {
		logger.Infof("proxy discovery is enabled, proxy addr is %s", proxyAddr)
		go proxy.Register(ctx, proxyAddr)
		go proxy.CleanExpiredProxies(ctx)
	}
	return proxy, nil
}

func (p *TiKVProxy) GetClusterID() uint64 {
	return p.clusterID
}

func (p *TiKVProxy) SetTestKey(ctx context.Context) error {
	_, err := p.Commit(ctx, &proxyv1.CommitRequest{
		Keys:   [][]byte{[]byte("__ctrip_test__")},
		Values: [][]byte{[]byte("__ctrip_test__")},
	})
	return err
}

func (p *TiKVProxy) HealthCheck(ctx context.Context) error {
	_, err := p.Get(ctx, &proxyv1.GetRequest{
		Key: []byte("__ctrip_test__"),
	})
	return err
}

func retry(ctx context.Context, fn func() error, maxRetry int) error {
	for {
		err := fn()
		if err == nil || !strings.Contains(err.Error(), "write conflict") {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if maxRetry > 0 {
			maxRetry--
		}
	}
}

func (p *TiKVProxy) setIfSmall(ctx context.Context, key string, wantSet uint64, diff uint64) (bool, error) {
	resp, err := p.Get(ctx, &proxyv1.GetRequest{
		Key: []byte(key),
	})
	if err != nil {
		return false, err
	}
	startTS := resp.StartTs
	var old uint64
	if len(resp.Value) != 8 {
		old = 0
	} else {
		old = binary.BigEndian.Uint64(resp.Value)
	}
	logger.Debugf("setIfSmall, key: %s, old: %d, wantSet: %d, diff: %d", key, old, wantSet, diff)
	if old < wantSet && wantSet-old > diff {
		ts := make([]byte, 8)
		binary.BigEndian.PutUint64(ts, wantSet)
		_, err := p.Commit(ctx, &proxyv1.CommitRequest{
			StartTs: startTS,
			Keys:    [][]byte{[]byte(key)},
			Values:  [][]byte{ts},
		})
		return err == nil, err
	}
	return false, nil
}

func (p *TiKVProxy) Register(ctx context.Context, proxyAddr string) error {
	timer := time.NewTicker(tikvProxySessionHeartbeatInterval)
	setActiveTime := func() error {
		currentTime := uint64(time.Now().Unix())
		sessionKey := fmt.Sprintf("%s%s", tikvProxySessionKeyPrefix, proxyAddr)
		commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		ts := make([]byte, 8)
		binary.BigEndian.PutUint64(ts, currentTime)
		_, err := p.Commit(commitCtx, &proxyv1.CommitRequest{
			Keys:   [][]byte{[]byte(sessionKey)},
			Values: [][]byte{ts},
		})
		if err != nil {
			return err
		}
		logger.Debugf("set active time for proxy %s, current time is %d", proxyAddr, currentTime)
		return nil
	}
	for {
		retry(ctx, setActiveTime, 5)
		select {
		case <-ctx.Done():
			logger.Infof("register context done")
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *TiKVProxy) Unregister(ctx context.Context, proxyAddr string) error {
	_, err := p.Commit(ctx, &proxyv1.CommitRequest{
		Keys:   [][]byte{[]byte(fmt.Sprintf("%s%s", tikvProxySessionKeyPrefix, proxyAddr))},
		Values: [][]byte{{}},
	})
	return err
}

func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	next := make([]byte, len(key))
	copy(next, key)
	p := len(next) - 1
	for {
		next[p]++
		if next[p] != 0 {
			break
		}
		p--
		if p < 0 {
			panic("can't scan keys for 0xFF")
		}
	}
	return next
}

func (p *TiKVProxy) GetAllProxies(ctx context.Context) (map[string]uint64, uint64, error) {
	logger.Debugf("get all proxies start")
	proxies := make(map[string]uint64)
	prefix := []byte(tikvProxySessionKeyPrefix)
	end := nextKey(prefix)
	resp, err := p.Scan(ctx, &proxyv1.ScanRequest{
		StartKey: prefix,
		EndKey:   end,
		ScanSize: math.MaxInt32,
	})
	logger.Debugf("get all proxies, startKey: %s, endKey: %s, resp: %v", string(prefix), string(end), resp)
	if err != nil {
		return nil, 0, err
	}

	for i, k := range resp.Keys {
		if len(resp.Values[i]) != 8 || len(k) <= len(prefix) {
			logger.Debugf("get all proxies, key: %s, value: %s, length: %d, prefix: %s", k, resp.Values[i], len(resp.Values[i]), prefix)
			continue
		}
		k = k[len(prefix):]
		proxies[string(k)] = binary.BigEndian.Uint64(resp.Values[i])
	}
	logger.Debugf("get all proxies end, proxies: %v, startTS: %d", proxies, resp.StartTs)
	return proxies, resp.StartTs, nil
}

func (p *TiKVProxy) CleanExpiredProxies(ctx context.Context) error {
	timer := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			logger.Infof("clean expired proxies context done")
			return ctx.Err()
		case <-timer.C:
			retry(ctx, func() error {
				now := uint64(time.Now().Unix())
				ok, err := p.setIfSmall(ctx, tikvProxySessionLastCleanKey, now, 15)
				if err != nil {
					return err
				}
				logger.Debugf("starting to clean expired proxies, now is %d, ok is %v", now, ok)
				if !ok {
					return nil
				}
				proxies, startTS, err := p.GetAllProxies(ctx)
				if err != nil {
					return err
				}
				needClean := make([][]byte, 0)
				for proxy, activeTime := range proxies {
					if activeTime < now-uint64(9*tikvProxySessionHeartbeatTimeout.Seconds()/10) {
						logger.Debugf("clean expired proxy %s, active time is %d, now is %d", proxy, activeTime, now)
						needClean = append(needClean, []byte(fmt.Sprintf("%s%s", tikvProxySessionKeyPrefix, proxy)))
					}
				}
				if len(needClean) != 0 {
					p.Commit(ctx, &proxyv1.CommitRequest{
						StartTs: startTS,
						Keys:    needClean,
						Values:  make([][]byte, len(needClean)),
					})
				}
				return nil
			}, 5)
		}
	}
}

func (p *TiKVProxy) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.closed {
			return
		}

		if p.cancel != nil {
			p.cancel()
		}

		if p.client != nil {
			err = p.client.Close()
		}

		p.closed = true
	})
	return err
}

// Get implements TxnProxyServiceServer.Get
func (p *TiKVProxy) Get(ctx context.Context, req *proxyv1.GetRequest) (*proxyv1.GetResponse, error) {
	startTS := req.StartTs
	var txn *tikv.KVTxn
	var err error
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
		}
		if startTS == math.MaxUint64 {
			txn.GetSnapshot().SetIsolationLevel(txnkv.RC) // RC isolation to skip lock checking in TiKV
		}
	} else {
		txn, err = p.client.Begin()
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

	value, err := txn.Get(ctx, req.Key)
	if tikverr.IsErrNotFound(err) {
		return &proxyv1.GetResponse{
			StartTs: txn.StartTS(),
			Value:   nil,
		}, nil
	}

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get key: %v", err)
	}
	logger.Debugf("get key: %s, value: %s, startTS: %d", req.Key, value, txn.StartTS())

	return &proxyv1.GetResponse{
		StartTs: txn.StartTS(),
		Value:   value,
	}, nil
}

// BatchGet implements TxnProxyServiceServer.BatchGet
func (p *TiKVProxy) BatchGet(ctx context.Context, req *proxyv1.BatchGetRequest) (*proxyv1.BatchGetResponse, error) {
	startTS := req.StartTs
	var txn *tikv.KVTxn
	var err error
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
		}
		if startTS == math.MaxUint64 {
			txn.GetSnapshot().SetIsolationLevel(txnkv.RC) // RC isolation to skip lock checking in TiKV
		}
	} else {
		txn, err = p.client.Begin()
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

	kvRes, err := txn.BatchGet(ctx, req.Keys)
	if err != nil {
		logger.Errorf("failed to batch get: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to batch get: %v", err)
	}

	keys := make([][]byte, 0, len(kvRes))
	values := make([][]byte, 0, len(kvRes))
	for key, value := range kvRes {
		keys = append(keys, []byte(key))
		values = append(values, value)
	}

	return &proxyv1.BatchGetResponse{
		StartTs: txn.StartTS(),
		Keys:    keys,
		Values:  values,
	}, nil
}

// Scan implements TxnProxyServiceServer.Scan
func (p *TiKVProxy) Scan(ctx context.Context, req *proxyv1.ScanRequest) (*proxyv1.ScanResponse, error) {
	startTS := req.StartTs

	var txn *tikv.KVTxn
	var err error
	scanSize := int(req.ScanSize)
	if scanSize > batchSize {
		// not allow to scan more than batchSize keys
		// client will do for loop to scan the rest keys
		scanSize = batchSize
	}

	if req.StartKey == nil {
		return nil, status.Errorf(codes.InvalidArgument, "start key is nil")
	}
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
		}
		if startTS == math.MaxUint64 {
			txn.GetSnapshot().SetIsolationLevel(txnkv.RC) // RC isolation to skip lock checking in TiKV
			if scanSize > 0 {
				txn.GetSnapshot().SetScanBatchSize(scanSize)
			}
			txn.GetSnapshot().SetNotFillCache(true)
		}
	} else {
		txn, err = p.client.Begin()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
		}
	}
	iter, err := txn.Iter(req.StartKey, req.EndKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create iterator: %v", err)
	}
	defer iter.Close()

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for iter.Valid() && len(keys) < scanSize {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, key)
		values = append(values, value)
		iter.Next()
	}

	return &proxyv1.ScanResponse{
		Keys:    keys,
		Values:  values,
		StartTs: txn.StartTS(),
		Eof:     !iter.Valid(),
	}, nil
}

// Commit implements TxnProxyServiceServer.Commit
func (p *TiKVProxy) Commit(ctx context.Context, req *proxyv1.CommitRequest) (*proxyv1.CommitResponse, error) {
	logger.Debugf("received commit request")
	allKeys := req.Keys
	allValues := req.Values

	// In a stateless proxy, the start_ts from the client is mainly for consistency checks on the client-side.
	// The proxy will start a new transaction for the commit.
	// We can still capture it for logging or potential future use.
	var startTS uint64 = req.StartTs

	if len(allKeys) != len(allValues) {
		logger.Errorf("keys and values length mismatch: %d != %d", len(allKeys), len(allValues))
		for _, k := range allKeys {
			logger.Debugf("commit key: %s", string(k))
		}
		for _, v := range allValues {
			logger.Debugf("commit value: %s", v)
		}
		return nil, status.Errorf(codes.Internal, "keys and values length mismatch: %d != %d", len(allKeys), len(allValues))
	}

	if len(allValues) == 0 {
		// empty transaction is not allowed
		logger.Warnf("committing an empty transaction for start_ts: %d", startTS)
		return nil, status.Errorf(codes.InvalidArgument, "committing an empty transaction for start_ts: %d", startTS)
	}

	var txn *tikv.KVTxn
	var err error
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
	} else {
		txn, err = p.client.Begin()
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

	txn.SetEnable1PC(true)

	for i, k := range allKeys {
		val := allValues[i]
		if len(val) == 0 {
			logger.Debugf("commit delete key: %s", string(k))
			err = txn.Delete(k)
		} else {
			logger.Debugf("commit set key: %s, value: %s", string(k), string(val))
			err = txn.Set(k, val)
		}
		if err != nil {
			// Best effort to rollback
			_ = txn.Rollback()
			return nil, status.Errorf(codes.Internal, "failed to commit key %s, value %s: %v", k, val, err)
		}
	}

	// Use a separate context for commit to ensure it completes even if client disconnects
	// We use background context with a reasonable timeout to prevent hanging indefinitely
	commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := txn.Commit(commitCtx); err != nil {
		logger.Errorf("failed to commit transaction for start_ts %d: %v", startTS, err)
		return nil, status.Errorf(codes.AlreadyExists, "failed to commit transaction for start_ts %d: %v", txn.StartTS(), err)
	}

	return &proxyv1.CommitResponse{
		CommitTs: txn.StartTS(), // Note: Using StartTS as a placeholder for CommitTS
	}, nil
}
