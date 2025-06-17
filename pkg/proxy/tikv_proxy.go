package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
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
	tikvProxySessionKeyPrefix = "__tikv_proxy_session__"
)

// TiKVProxy implements proxy for tikv with transaction management
type TiKVProxy struct {
	client *txnkv.Client
	proxyv1.UnimplementedTxnProxyServiceServer
}

var logger = utils.GetLogger("juicefs")

func NewTiKVProxy(addr string) (*TiKVProxy, error) {
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

	interval := time.Hour * 3
	if dur, err := time.ParseDuration(query.Get("gc-interval")); err == nil {
		if dur != 0 && dur < time.Hour {
			logger.Warnf("TiKV gc-interval (%s) is too short, and is reset to 1h", dur)
			dur = time.Hour
		}
		interval = dur
	}
	logger.Infof("TiKV gc interval is set to %s", interval)
	logger.Infof("TiKV addr is %s", addr)

	client, err := txnkv.NewClient(strings.Split(addr, ","))
	if err != nil {
		return nil, err
	}

	proxy := &TiKVProxy{
		client: client,
	}

	return proxy, nil
}

func (p *TiKVProxy) SetTestKey(ctx context.Context) error {
	txn, err := p.client.Begin()
	if err != nil {
		return err
	}
	err = txn.Set([]byte("__ctrip_test__"), []byte("__test__"))
	if err != nil {
		return err
	}
	if err := txn.Commit(ctx); err != nil {
		return err
	}
	logger.Debugf("set test key: __ctrip_test__")
	return nil
}

func (p *TiKVProxy) HealthCheck(ctx context.Context) error {
	txn, err := p.client.Begin()
	if err != nil {
		return err
	}
	value, err := txn.Get(ctx, []byte("__ctrip_test__"))
	if tikverr.IsErrNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	logger.Debugf("get test key: __ctrip_test__, value: %s", value)
	return nil
}

func (p *TiKVProxy) Register(ctx context.Context, proxyAddr string) error {
	retry := func(ctx context.Context, fn func() error, maxRetry int) error {
		for {
			err := fn()
			if err == nil {
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

	go func() {
		timer := time.NewTicker(time.Second * 3)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				setActiveTime := func() error {
					currentTime := int64(time.Now().Unix())
					sessionKey := fmt.Sprintf("%s/%s", tikvProxySessionKeyPrefix, proxyAddr)
					commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
					defer cancel()
					ts := make([]byte, 8)
					binary.BigEndian.PutUint64(ts, uint64(currentTime))
					_, err := p.Commit(commitCtx, &proxyv1.CommitRequest{
						Keys:   [][]byte{[]byte(sessionKey)},
						Values: [][]byte{ts},
					})
					if err != nil {
						return err
					}
					return nil
				}
				retry(ctx, setActiveTime, 5)
			}
		}
	}()
	return nil
}

func (p *TiKVProxy) Unregister(ctx context.Context, proxyAddr string) error {
	txn, err := p.client.Begin()
	if err != nil {
		return err
	}
	err = txn.Delete([]byte(fmt.Sprintf("%s/%s", tikvProxySessionKeyPrefix, proxyAddr)))
	if err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func (p *TiKVProxy) GetAllProxies(ctx context.Context) (map[string]uint64, error) {
	proxies := make(map[string]uint64)
	prefix := []byte(tikvProxySessionKeyPrefix)
	end := make([]byte, len(prefix))
	end[len(end)-1]++
	resp, err := p.Scan(ctx, &proxyv1.ScanRequest{
		StartTs:  math.MaxUint64,
		StartKey: prefix,
		EndKey:   end,
	})
	if err != nil {
		return nil, err
	}

	for i, k := range resp.Keys {
		if len(resp.Values[i]) != 8 {
			continue
		}
	for iter.Valid() {
		if len(iter.Value()) != 8 {
			iter.Next()
			continue
		}
		proxies[string(iter.Key())] = binary.BigEndian.Uint64(iter.Value())
		iter.Next()
	}
	return proxies, nil
}

func (p *TiKVProxy) CleanExpiredProxies(ctx context.Context, expiredTime uint64) error {
	proxies, err := p.GetAllProxies(ctx)
	if err != nil {
		return err
	}
	for proxy, activeTime := range proxies {
		if activeTime < expiredTime {
			err = p.Unregister(ctx, proxy)
			if err != nil {
				continue
			}
		}
	}
	return nil
}

func (p *TiKVProxy) Close() error {
	return p.client.Close()
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
		// TiKV disallows empty transactions, but we can treat this as a successful no-op.
		// A read-only transaction might not have a valid commit_ts, so we can't create one.
		// However, the client expects a response.
		// A better approach might be to define what an empty commit means.
		// For now, we return an empty response, but the client needs to handle it.
		// A real commit_ts is needed, so we must perform a transaction.
		// Let's create a transaction and commit it to get a valid commitTS.
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
			err = txn.Delete(k)
		} else {
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
		return nil, status.Errorf(codes.Internal, "failed to commit transaction for start_ts %d: %v", txn.StartTS(), err)
	}

	return &proxyv1.CommitResponse{
		CommitTs: txn.StartTS(), // Note: Using StartTS as a placeholder for CommitTS
	}, nil
}
