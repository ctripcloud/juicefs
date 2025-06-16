//go:build !notikv
// +build !notikv

/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func init() {
	Register("tikv-proxy", newKVMeta)
	drivers["tikv-proxy"] = newTikvProxyClient
}

type tikvProxyTxn struct {
	client  proxyv1.TxnProxyServiceClient
	startTS uint64
	writes  map[string][]byte
	reads   map[string][]byte
}

func (tx *tikvProxyTxn) get(key []byte) []byte {
	logger.Debugf("get key: %s, startTS: %d", string(key), tx.startTS)
	if v, ok := tx.writes[string(key)]; ok {
		logger.Debugf("because of deleted key, return nil")
		return v
	}

	if v, ok := tx.reads[string(key)]; ok {
		logger.Debugf("because of ready get key, return nil")
		return v
	}

	resp, err := tx.client.Get(context.TODO(), &proxyv1.GetRequest{
		StartTs: tx.startTS,
		Key:     key,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound { // NotFound
			logger.Debugf("because of not found, return nil")
			return nil
		}
		panic(err)
	}
	if tx.startTS == 0 {
		tx.startTS = resp.StartTs
	}

	tx.reads[string(key)] = resp.Value
	if len(resp.Value) == 0 {
		logger.Debugf("because of empty value, return nil")
	}
	return resp.Value
}

func (tx *tikvProxyTxn) gets(keys ...[]byte) [][]byte {
	logger.Debugf("gets keys: %v, startTS: %d", keys, tx.startTS)
	values := make([][]byte, len(keys))
	remoteKeys := make([][]byte, 0, len(keys))
	remoteIndex := make(map[string]int)

	// First, check local buffer buffer
	for i, key := range keys {
		keyStr := string(key)
		if v, ok := tx.writes[keyStr]; ok {
			values[i] = v
			continue
		}
		if v, ok := tx.reads[keyStr]; ok {
			values[i] = v
			continue
		}
		// Key not found in local buffer, need to fetch from remote
		remoteKeys = append(remoteKeys, key)
		remoteIndex[string(key)] = i
	}

	// If we have keys to fetch from remote
	if len(remoteKeys) > 0 {
		resp, err := tx.client.BatchGet(context.TODO(), &proxyv1.BatchGetRequest{
			StartTs: tx.startTS,
			Keys:    remoteKeys,
		})
		if err != nil {
			logger.Errorf("failed to batch get: %v", err)
			panic(err)
		}
		for i, key := range resp.Keys {
			values[remoteIndex[string(key)]] = resp.Values[i]
			tx.reads[string(key)] = resp.Values[i]
		}
		if tx.startTS == 0 {
			tx.startTS = resp.StartTs
		}
	}
	return values
}

func (tx *tikvProxyTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {

	logger.Debugf("scan begin: %s, end: %s, startTS: %d", string(begin), string(end), tx.startTS)
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := tx.client.Scan(streamCtx, &proxyv1.ScanRequest{
		StartTs:  tx.startTS,
		StartKey: begin,
		EndKey:   end,
		ScanSize: math.MaxInt32, // Default scan size
	})
	if err != nil {
		panic(err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if tx.startTS == 0 {
			tx.startTS = resp.StartTs
		}
		for i, k := range resp.Keys {
			if !handler(k, resp.Values[i]) {
				return
			}
		}
	}
}

func (tx *tikvProxyTxn) exist(prefix []byte) bool {
	logger.Debugf("exist prefix: %s, startTS: %d", string(prefix), tx.startTS)
	stream, err := tx.client.Scan(context.TODO(), &proxyv1.ScanRequest{
		StartTs:  tx.startTS,
		StartKey: prefix,
		EndKey:   nextKey(prefix),
		ScanSize: 0,
	})
	if err != nil {
		panic(err)
	}
	resp, err := stream.Recv()
	if err == io.EOF {
		logger.Debugf("scan eof in exist, prefix: %s", string(prefix))
		return false
	}

	if err != nil {
		panic(err)
	}

	if tx.startTS == 0 {
		tx.startTS = resp.StartTs
	}

	if resp != nil && len(resp.Keys) > 0 {
		logger.Debugf("scan found in exist")
		stream.CloseSend()
		return true
	}

	logger.Debugf("No key found in exist, prefix: %s", string(prefix))

	return false
}

func (tx *tikvProxyTxn) set(key, value []byte) {
	if tx.writes == nil {
		tx.writes = make(map[string][]byte)
	}
	tx.writes[string(key)] = value
}

func (tx *tikvProxyTxn) append(key []byte, value []byte) {
	logger.Debugf("append key: %s, value: %s, startTS: %d", string(key), string(value), tx.startTS)
	existing := tx.get(key)
	newValue := append(existing, value...)
	tx.set(key, newValue)
}

func (tx *tikvProxyTxn) incrBy(key []byte, value int64) int64 {
	logger.Debugf("incrBy key: %s, value: %d, startTS: %d", string(key), value, tx.startTS)
	existing := tx.get(key)
	new := parseCounter(existing)
	if value != 0 {
		new += value
		tx.set(key, packCounter(new))
	}
	return new
}

func (tx *tikvProxyTxn) delete(key []byte) {
	logger.Debugf("delete key: %s, startTS: %d", string(key), tx.startTS)
	if tx.writes == nil {
		tx.writes = make(map[string][]byte)
	}
	tx.writes[string(key)] = []byte{}
}

func (tx *tikvProxyTxn) commit() error {
	logger.Debugf("Commit startTS: %d", tx.startTS)
	if len(tx.writes) == 0 {
		logger.Debugf("no buffer to commit")
		return nil // No buffer to commit
	}
	keys := make([][]byte, 0, len(tx.writes))
	values := make([][]byte, 0, len(tx.writes))
	for k, v := range tx.writes {
		keys = append(keys, []byte(k))
		values = append(values, v)
	}

	_, err := tx.client.Commit(context.TODO(), &proxyv1.CommitRequest{
		StartTs: tx.startTS,
		Keys:    keys,
		Values:  values,
	})

	return err
}

type tikvProxyClient struct {
	conn   *grpc.ClientConn
	client proxyv1.TxnProxyServiceClient
	addr   string
}

func newTikvProxyClient(addr string) (tkvClient, error) {
	// Parse the address to extract the gRPC server address
	logger.Infof("TiKV Proxy addr is %s", addr)
	tUrl, err := url.Parse("tikv-proxy://" + addr)
	if err != nil {
		return nil, err
	}

	// Connect to the TiKV Proxy gRPC server
	conn, err := grpc.NewClient(tUrl.Host, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(`{
		"loadBalancingPolicy": "round_robin"
	}`), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TiKV Proxy at %s: %v", tUrl.Host, err)
	}

	client := proxyv1.NewTxnProxyServiceClient(conn)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Get(ctx, &proxyv1.GetRequest{
		StartTs: math.MaxUint64, // Use max uint64 for read-only test
		Key:     []byte("__test__"),
	})
	if err != nil && status.Code(err) != 5 { // Ignore NotFound errors
		conn.Close()
		return nil, fmt.Errorf("failed to test TiKV Proxy connection: %v", err)
	}

	logger.Infof("Connected to TiKV Proxy at %s", tUrl.Host)

	prefix := strings.TrimLeft(tUrl.Path, "/")
	return withPrefix(&tikvProxyClient{
		conn:   conn,
		client: client,
		addr:   addr,
	}, append([]byte(prefix), 0xFD)), nil
}

func (c *tikvProxyClient) name() string {
	return "tikv-proxy"
}

func (c *tikvProxyClient) shouldRetry(err error) bool {
	// For gRPC errors, we can retry on certain conditions
	return strings.Contains(err.Error(), "write conflict") || strings.Contains(err.Error(), "TxnLockNotFound")
}

func (c *tikvProxyClient) txn(f func(*kvTxn) error, retry int) (err error) {
	// Begin a new transaction by getting a start timestamp
	// For simplicity, we'll use current time as start timestamp
	// In a real implementation, this should come from PD

	proxyTxn := &tikvProxyTxn{
		client:  c.client,
		startTS: 0,
		writes:  make(map[string][]byte),
		reads:   make(map[string][]byte),
	}
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(error)
			if ok {
				err = fe
			} else {
				err = errors.Errorf("tikv-proxy client txn func error: %v", r)
			}
		}
	}()
	err = f(&kvTxn{proxyTxn, retry})
	if err != nil {
		return err
	}

	// Commit the transaction
	err = proxyTxn.commit()

	return err
}

func (c *tikvProxyClient) scan(prefix []byte, handler func(key, value []byte)) error {
	startTS := uint64(time.Now().UnixNano())

	stream, err := c.client.Scan(context.TODO(), &proxyv1.ScanRequest{
		StartTs:  startTS,
		StartKey: prefix,
		EndKey:   nextKey(prefix),
		ScanSize: 1000,
	})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for i, k := range resp.Keys {
			handler(k, resp.Values[i])
		}
	}
	return nil
}

func (c *tikvProxyClient) reset(prefix []byte) error {
	// Reset by deleting all keys with the given prefix
	return c.txn(func(tx *kvTxn) error {
		tx.deleteKeys(prefix)
		return nil
	}, 3)
}

func (c *tikvProxyClient) close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *tikvProxyClient) gc() {
	// GC is handled by the TiKV Proxy server, nothing to do here
	logger.Debug("TiKV Proxy client GC called (no-op)")
}
