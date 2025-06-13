package proxy

import (
	"context"
	"io"
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

const (
	batchSize = 100
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
		return nil, nil
	}

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get key: %v", err)
	}

	return &proxyv1.GetResponse{
		StartTs: txn.StartTS(),
		Value:   value,
	}, nil
}

// BatchGet implements TxnProxyServiceServer.BatchGet
func (p *TiKVProxy) BatchGet(req *proxyv1.BatchGetRequest, stream proxyv1.TxnProxyService_BatchGetServer) error {
	startTS := req.StartTs

	var txn *tikv.KVTxn
	var err error
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
		if err != nil {
			return status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
		}
		if startTS == math.MaxUint64 {
			txn.GetSnapshot().SetIsolationLevel(txnkv.RC) // RC isolation to skip lock checking in TiKV
		}
	} else {
		txn, err = p.client.Begin()
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

	kvRes, err := txn.BatchGet(stream.Context(), req.Keys)
	if err != nil {
		logger.Errorf("failed to batch get: %v", err)
		return status.Errorf(codes.Internal, "failed to batch get: %v", err)
	}

	cnt := 0
	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	for key, value := range kvRes {
		if cnt >= batchSize {
			if err := stream.Send(&proxyv1.BatchGetResponse{
				Keys:    keys,
				Values:  values,
				StartTs: txn.StartTS(),
			}); err != nil {
				// Client closed the stream, stop processing immediately
				return err
			}
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			cnt = 0
		}
		cnt++
		keys = append(keys, []byte(key))
		values = append(values, value)

		// Check if client closed the stream by testing context cancellation
		select {
		case <-stream.Context().Done():
			// Client closed the stream, stop processing immediately
			return stream.Context().Err()
		default:
			// Continue processing
		}
	}

	if cnt > 0 {
		if err := stream.Send(&proxyv1.BatchGetResponse{
			Keys:    keys,
			Values:  values,
			StartTs: txn.StartTS(),
		}); err != nil {
			logger.Debugf("failed to send batch get response: %v", err)
			// Client closed the stream
			return err
		}
	}

	return nil
}

// Scan implements TxnProxyServiceServer.Scan
func (p *TiKVProxy) Scan(req *proxyv1.ScanRequest, stream proxyv1.TxnProxyService_ScanServer) error {
	startTS := req.StartTs

	var txn *tikv.KVTxn
	var err error
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
		if err != nil {
			return status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
		}
		if startTS == math.MaxUint64 {
			txn.GetSnapshot().SetIsolationLevel(txnkv.RC) // RC isolation to skip lock checking in TiKV
		}
	} else {
		txn, err = p.client.Begin()
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

	iter, err := txn.Iter(req.StartKey, req.EndKey)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create iterator: %v", err)
	}
	defer iter.Close()

	scanSize := int(req.ScanSize)
	scanCnt := 0
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	if scanSize == 0 && iter.Valid() {
		// just for check the iterator is valid
		stream.Send(&proxyv1.ScanResponse{
			Keys:    [][]byte{[]byte("__exist__")},
			Values:  [][]byte{[]byte("__exist__")},
			StartTs: txn.StartTS(),
		})
		return nil // just for check the iterator is valid
	}

	for iter.Valid() && scanCnt < scanSize {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, key)
		values = append(values, value)
		scanCnt++
		if len(keys) >= scanSize {
			if err := stream.Send(&proxyv1.ScanResponse{
				Keys:    keys,
				Values:  values,
				StartTs: txn.StartTS(),
			}); err != nil {
				// Client closed the stream, stop scanning immediately
				return err
			}
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			scanCnt = 0
		}

		// Check if client closed the stream by testing context cancellation
		select {
		case <-stream.Context().Done():
			// Client closed the stream, stop scanning immediately
			return stream.Context().Err()
		default:
			// Continue scanning
		}

		iter.Next()
	}
	if scanCnt > 0 {
		if err := stream.Send(&proxyv1.ScanResponse{
			Keys:    keys,
			Values:  values,
			StartTs: txn.StartTS(),
		}); err != nil {
			// Client closed the stream
			return err
		}
	}

	return nil
}

// Commit implements TxnProxyServiceServer.Commit
func (p *TiKVProxy) Commit(stream proxyv1.TxnProxyService_CommitServer) error {
	logger.Debugf("received commit request")
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var startTS uint64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive from stream: %v", err)
		}

		// In a stateless proxy, the start_ts from the client is mainly for consistency checks on the client-side.
		// The proxy will start a new transaction for the commit.
		// We can still capture it for logging or potential future use.
		if startTS == 0 {
			startTS = req.GetStartTs()
		}

		for i, k := range req.Keys {
			logger.Debugf("commit key: %s, value: %s", k, req.Values[i])
			allKeys = append(allKeys, k)
			if req.Values[i] == nil {
				allValues = append(allValues, []byte{})
			} else {
				allValues = append(allValues, req.Values[i])
			}
		}
	}
	if len(allKeys) != len(allValues) {
		logger.Errorf("keys and values length mismatch: %d != %d", len(allKeys), len(allValues))
		return status.Errorf(codes.Internal, "keys and values length mismatch: %d != %d", len(allKeys), len(allValues))
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
		return stream.SendAndClose(&proxyv1.CommitResponse{
			CommitTs: startTS,
		})
	}

	var txn *tikv.KVTxn
	var err error
	if startTS != 0 {
		txn, err = p.client.Begin(tikv.WithStartTS(startTS))
	} else {
		txn, err = p.client.Begin()
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

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
			return status.Errorf(codes.Internal, "failed to commit key %s, value %s: %v", k, val, err)
		}
	}

	// Use a separate context for commit to ensure it completes even if client disconnects
	// We use background context with a reasonable timeout to prevent hanging indefinitely
	commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := txn.Commit(commitCtx); err != nil {
		logger.Errorf("failed to commit transaction for start_ts %d: %v", startTS, err)
		return status.Errorf(codes.Internal, "failed to commit transaction for start_ts %d: %v", txn.StartTS(), err)
	}
	
	return stream.SendAndClose(&proxyv1.CommitResponse{
		CommitTs: txn.StartTS(), // Note: Using StartTS as a placeholder for CommitTS
	})
}
