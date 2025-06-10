package proxy

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
	"github.com/sirupsen/logrus"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	batchSize = 100
)

type Transaction struct {
	tikv     *tikv.KVTxn
	startTS  uint64
	commitTS uint64
	created  time.Time
}

// TiKVProxy implements proxy for tikv with transaction management
type TiKVProxy struct {
	client       *txnkv.Client
	transactions *sync.Map // map[string]*Transaction
	cleanupTick  *time.Ticker
	stopCh       chan struct{}
	logger       *logrus.Entry
	proxyv1.UnimplementedTxnProxyServiceServer
}

func NewTiKVProxy(addr string) (*TiKVProxy, error) {
	client, err := txnkv.NewClient(strings.Split(addr, ","))
	if err != nil {
		return nil, err
	}

	proxy := &TiKVProxy{
		client:       client,
		transactions: &sync.Map{},
		cleanupTick:  time.NewTicker(5 * time.Minute), // cleanup every 5 minutes
		stopCh:       make(chan struct{}),
		logger:       logrus.WithField("component", "tikv-proxy"),
	}

	// Start cleanup routine for expired transactions
	go proxy.cleanupExpiredTransactions()

	return proxy, nil
}

func (p *TiKVProxy) Close() error {
	if p.cleanupTick != nil {
		p.cleanupTick.Stop()
	}

	// Clean up all active transactions
	p.transactions.Range(func(key, value interface{}) bool {
		p.transactions.Delete(key)
		return true
	})

	return p.client.Close()
}

// BeginTxn implements TxnProxyServiceServer.BeginTxn
func (p *TiKVProxy) BeginTxn(ctx context.Context, req *proxyv1.BeginTxnRequest) (*proxyv1.BeginTxnResponse, error) {
	txnID, err := p.beginTxnInternal(req.ProposalUuid)
	if err != nil {
		return &proxyv1.BeginTxnResponse{
		}, nil
	}

	return &proxyv1.BeginTxnResponse{
		TxnId: txnID,
	}, nil
}

// Get implements TxnProxyServiceServer.Get
func (p *TiKVProxy) Get(ctx context.Context, req *proxyv1.GetRequest) (*proxyv1.GetResponse, error) {
	txn, txnID, err := p.getOrCreateTransaction(req.TxnId)
	if err != nil {
		return nil, err
	}

	// Get from TiKV
	value, err := txn.tikv.Get(ctx, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get key: %v", err)
	}

	// Auto commit if requested
	if req.AutoCommit {
		if commitErr := p.commitTxn(ctx, txnID); commitErr != nil {
			return &proxyv1.GetResponse{
				Value:        value,
				TxnId:        txnID,
			}, nil
		}
	}

	return &proxyv1.GetResponse{
		Value: value,
		TxnId: txnID,
	}, nil
}

// Set implements TxnProxyServiceServer.Set
func (p *TiKVProxy) Set(ctx context.Context, req *proxyv1.SetRequest) (*proxyv1.SetResponse, error) {
	txn, txnID, err := p.getOrCreateTransaction(req.TxnId)
	if err != nil {
		return nil, err
	}

	err = txn.tikv.Set(req.Key, req.Value)
	if err != nil {
		return &proxyv1.SetResponse{
			TxnId:        req.TxnId,
		}, nil
	}

	return &proxyv1.SetResponse{
		TxnId: txnID,
	}, nil

}

// BatchGet implements TxnProxyServiceServer.BatchGet
func (p *TiKVProxy) BatchGet(req *proxyv1.BatchGetRequest, stream proxyv1.TxnProxyService_BatchGetServer) error {
	txn, txnID, err := p.getOrCreateTransaction(req.TxnId)
	if err != nil {
		return err
	}

	values, err := txn.tikv.BatchGet(stream.Context(), req.Keys)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to batch get: %v", err)
	}

	cnt := 0
	batch := make(map[string][]byte)
	for key, value := range values {
		if cnt >= batchSize {
			if err := stream.Send(&proxyv1.BatchGetResponse{
				Values: batch,
				TxnId:  txnID,
			}); err != nil {
				return err
			}
			batch = make(map[string][]byte)
			cnt = 0
		}
		cnt++
		batch[string(key)] = value
	}

	if cnt > 0 {
		if err := stream.Send(&proxyv1.BatchGetResponse{
			Values: batch,
			TxnId:  txnID,
		}); err != nil {
			return err
		}
	}

	// Auto commit if requested
	if req.AutoCommit {
		if commitErr := p.commitTxn(stream.Context(), txnID); commitErr != nil {
			p.logger.Errorf("Auto commit failed for transaction %s: %v", txnID, commitErr)
		}
	}

	return nil
}

// Delete implements TxnProxyServiceServer.Delete
func (p *TiKVProxy) Delete(ctx context.Context, req *proxyv1.DeleteRequest) (*proxyv1.DeleteResponse, error) {
	// TODO: implement delete logic
	txn, txnID, err := p.getOrCreateTransaction(req.TxnId)
	if err != nil {
		return &proxyv1.DeleteResponse{
			TxnId:        req.TxnId,
		}, status.Errorf(codes.Internal, err.Error())
	}
	err = txn.tikv.Delete(req.Key)
	if err != nil {
		return &proxyv1.DeleteResponse{
			TxnId:        req.TxnId,
		}, status.Errorf(codes.Internal, err.Error())
	}

	if req.AutoCommit {
		if commitErr := p.commitTxn(ctx, txnID); commitErr != nil {
			return &proxyv1.DeleteResponse{
				TxnId:        req.TxnId,
			}, status.Errorf(codes.Internal, commitErr.Error())
		}
	}
	return &proxyv1.DeleteResponse{
		TxnId:        req.TxnId,
	}, nil
}

// Scan implements TxnProxyServiceServer.Scan
func (p *TiKVProxy) Scan(req *proxyv1.ScanRequest, stream proxyv1.TxnProxyService_ScanServer) error {
	txn, txnID, err := p.getOrCreateTransaction(req.TxnId)
	if err != nil {
		return err
	}
	iter, err := txn.tikv.Iter(req.StartKey, req.EndKey)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create iterator: %v", err)
	}
	defer iter.Close()

	scanSize := int(req.ScanSize)
	scanCnt := 0
	response := make(map[string][]byte)

	if scanSize == 0 && iter.Valid() {
		return nil // just for check the iterator is valid
	}

	for iter.Valid() && scanCnt < scanSize {
		key := iter.Key()
		value := iter.Value()
		response[string(key)] = value
		scanCnt++
		if len(response) >= scanSize {
			if err := stream.Send(&proxyv1.ScanResponse{
				Values: response,
				TxnId:  txnID,
			}); err != nil {
				return err
			}
			response = make(map[string][]byte)
			scanCnt = 0
		}
		iter.Next()
	}
	if scanCnt > 0 {
		if err := stream.Send(&proxyv1.ScanResponse{
			Values: response,
			TxnId:  txnID,
		}); err != nil {
			return status.Errorf(codes.Internal, err.Error())
		}
	}

	return nil
}

func (p *TiKVProxy) ScanSnap(req *proxyv1.ScanSnapRequest, stream proxyv1.TxnProxyService_ScanSnapServer) error {
	//TODO: implement scan snapshot
	ts, err := p.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snap := p.client.GetSnapshot(ts)
	snap.SetScanBatchSize(10240)
	snap.SetNotFillCache(true)
	snap.SetPriority(txnutil.PriorityLow)

	iter, err := snap.Iter(req.StartKey, req.EndKey)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create iterator: %v", err)
	}
	defer iter.Close()

	scanSize := int(req.ScanSize)
	if scanSize == -1{
		scanSize = int(math.MaxInt32)
	}
	scanCnt := 0
	response := make(map[string][]byte)

	if scanSize == 0 && iter.Valid() {
		return nil // just for check the iterator is valid
	}
	
	for iter.Valid() && scanCnt < scanSize {
		key := iter.Key()
		value := iter.Value()
		response[string(key)] = value
		scanCnt++
		if len(response) >= scanSize {
			if err := stream.Send(&proxyv1.ScanSnapResponse{
				Values: response,
			}); err != nil {
				return err
			}
			response = make(map[string][]byte)
			scanCnt = 0
		}
		iter.Next()
	}
	if scanCnt > 0 {
		if err := stream.Send(&proxyv1.ScanSnapResponse{
			Values: response,
		}); err != nil {
			return err
		}
	}	
	return nil
}

// Commit implements TxnProxyServiceServer.Commit
func (p *TiKVProxy) Commit(ctx context.Context, req *proxyv1.CommitRequest) (*proxyv1.CommitResponse, error) {
	txn, txnID, err := p.getOrCreateTransaction(req.TxnId)
	if err != nil {
		return &proxyv1.CommitResponse{
		}, status.Errorf(codes.Internal, err.Error())
	}

	for key, value := range req.Values {
		txn.tikv.Set([]byte(key), value)
	}

	if err := p.commitTxn(ctx, txnID); err != nil {
		return &proxyv1.CommitResponse{
		}, status.Errorf(codes.Internal, err.Error())
	}

	return &proxyv1.CommitResponse{
	}, nil
}

// Rollback implements TxnProxyServiceServer.Rollback
func (p *TiKVProxy) Rollback(ctx context.Context, req *proxyv1.RollbackRequest) (*proxyv1.RollbackResponse, error) {
	// TODO: implement rollback logic
	return &proxyv1.RollbackResponse{
	}, nil
}

// beginTxnInternal creates a new transaction and returns its UUID
func (p *TiKVProxy) beginTxnInternal(proposalUUID string) (string, error) {
	tikvTxn, err := p.client.KVStore.Begin()
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to begin tikv transaction: %v", err)
	}

	var txnID string
	if proposalUUID != "" {
		txnID = proposalUUID
	} else {
		txnID = uuid.New().String()
	}

	txn := &Transaction{
		tikv:    tikvTxn,
		startTS: tikvTxn.StartTS(),
		created: time.Now(),
	}

	p.transactions.Store(txnID, txn)
	p.logger.Debugf("Created transaction %s", txnID)

	return txnID, nil
}

// getTransaction retrieves a transaction by ID
func (p *TiKVProxy) getTransaction(txnID string) (*Transaction, error) {
	if txnID == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction ID is required")
	}

	value, ok := p.transactions.Load(txnID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "transaction %s not found", txnID)
	}

	return value.(*Transaction), nil
}

// getOrCreateTransaction creates a new transaction if txnID is empty, otherwise retrieves an existing transaction
func (p *TiKVProxy) getOrCreateTransaction(txnID string) (*Transaction, string, error) {
	if txnID == "" {
		var err error
		txnID, err = p.beginTxnInternal("")
		if err != nil {
			return nil, "", err
		}
	}

	txn, err := p.getTransaction(txnID)
	if err != nil {
		return nil, "", err
	}
	return txn, txnID, nil
}

// commitTxn commits a transaction and cleans up
func (p *TiKVProxy) commitTxn(ctx context.Context, txnID string) error {

	txn, err := p.getTransaction(txnID)
	if err != nil {
		return err
	}

	if !txn.tikv.IsReadOnly() {
		txn.tikv.SetEnable1PC(true)
		txn.tikv.SetEnableAsyncCommit(true)
		err = txn.tikv.Commit(ctx)
	}

	txn.commitTS = txn.tikv.StartTS() //TODO: use the commitTS not startTS
	p.cleanupTxn(txnID)
	if err != nil {
		return err
	}
	return nil
}

// cleanupTxn cleans up a transaction
func (p *TiKVProxy) cleanupTxn(txnID string) {
	if _, ok := p.transactions.Load(txnID); ok {
		p.logger.Debugf("Cleaned up transaction %s", txnID)
		p.transactions.Delete(txnID)
	}
}

// rollbackInternal rolls back a transaction
func (p *TiKVProxy) rollbackInternal(ctx context.Context, txnID string) error {
	// TODO: implement rollback transaction
	return nil
}

// cleanupExpiredTransactions periodically cleans up expired transactions
func (p *TiKVProxy) cleanupExpiredTransactions() {
	for {
		select {
		case <-p.cleanupTick.C:
			p.transactions.Range(func(key, value interface{}) bool {
				txn := value.(*Transaction)
				if time.Now().After(txn.created.Add(5 * time.Minute)) {
					// we not allow the transaction start more than 5 minutes
					p.cleanupTxn(key.(string))
				}
				return true
			})
		case <-p.stopCh:
			return
		}
	}
}
