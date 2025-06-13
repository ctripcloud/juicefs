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

	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
	"google.golang.org/grpc"
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
	retry   int
}

func (tx *tikvProxyTxn) get(key []byte) []byte {
	if tx.writes != nil {
		if v, ok := tx.writes[string(key)]; ok {
			if len(v) == 0 {
				return nil
			}
			return v
		}
	}
	resp, err := tx.client.Get(context.TODO(), &proxyv1.GetRequest{
		StartTs: tx.startTS,
		Key:     key,
	})
	if err != nil {
		if status.Code(err) == 5 { // NotFound
			return nil
		}
		panic(err)
	}
	if resp == nil {
		return nil
	}
	if tx.startTS == 0 {
		tx.startTS = resp.StartTs
	}
	return resp.Value
}

func (tx *tikvProxyTxn) gets(keys ...[]byte) [][]byte {

	values := make([][]byte, len(keys))
	remoteIndexes := make(map[string]int)
	remoteKeys := make([][]byte, 0, len(keys))

	// First, check local writes buffer
	for i, key := range keys {
		keyStr := string(key)
		if tx.writes != nil {
			if v, ok := tx.writes[keyStr]; ok {
				if len(v) == 0 {
					values[i] = nil // Deleted key
				} else {
					values[i] = v
				}
				continue
			}
		}
		// Key not found in local buffer, need to fetch from remote
		remoteIndexes[keyStr] = i
		remoteKeys = append(remoteKeys, key)
	}

	// If we have keys to fetch from remote
	if len(remoteIndexes) > 0 {
		stream, err := tx.client.BatchGet(context.TODO(), &proxyv1.BatchGetRequest{
			StartTs: tx.startTS,
			Keys:    remoteKeys,
		})
		if err != nil {
			logger.Errorf("failed to batch get: %v", err)
			panic(err)
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Errorf("failed to batch get: %v", err)
				panic(err)
			}
			for i, key := range resp.Keys {
				originalIndex := remoteIndexes[string(key)]
				values[originalIndex] = resp.Values[i]
			}
			if tx.startTS == 0 {
				tx.startTS = resp.StartTs
			}
		}
	}

	return values
}

func (tx *tikvProxyTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {

	stream, err := tx.client.Scan(context.TODO(), &proxyv1.ScanRequest{
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
		for i, k := range resp.Keys {
			if !handler(k, resp.Values[i]) {
				stream.CloseSend()
				return
			}
		}
	}
}

func (tx *tikvProxyTxn) exist(prefix []byte) bool {
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

	if resp != nil && len(resp.Keys) > 0 {
		return true
	}

	if err == io.EOF {
		return false
	}

	if err != nil {
		panic(err)
	}

	return false
}

func (tx *tikvProxyTxn) set(key, value []byte) {
	if tx.writes == nil {
		tx.writes = make(map[string][]byte)
	}
	tx.writes[string(key)] = value
}

func (tx *tikvProxyTxn) append(key []byte, value []byte) {
	existing := tx.get(key)
	newValue := append(existing, value...)
	tx.set(key, newValue)
}

func (tx *tikvProxyTxn) incrBy(key []byte, value int64) int64 {
	existing := tx.get(key)
	var current int64
	if len(existing) == 8 {
		current = parseCounter(existing)
	}
	newValue := current + value
	tx.set(key, packCounter(newValue))
	return newValue
}

func (tx *tikvProxyTxn) delete(key []byte) {
	if tx.writes == nil {
		tx.writes = make(map[string][]byte)
	}
	tx.writes[string(key)] = []byte{}
}

func (tx *tikvProxyTxn) commit() error {
	if len(tx.writes) == 0 {
		logger.Debugf("no writes to commit")
		return nil // No writes to commit
	}
	for k, v := range tx.writes {
		logger.Debugf("commit key: %s, value: %s", k, v)
	}

	stream, err := tx.client.Commit(context.TODO())
	if err != nil {
		return err
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	for k, v := range tx.writes {
		keys = append(keys, []byte(k))
		values = append(values, v)
	}
	// Send all writes in a single request
	err = stream.Send(&proxyv1.CommitRequest{
		StartTs: tx.startTS,
		Keys:    keys,
		Values:  values,
	})
	if err != nil {
		return err
	}

	// Close the send side and wait for server response
	// CloseAndRecv() closes the send side and waits for the server to close the stream
	_, err = stream.CloseAndRecv()
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
	conn, err := grpc.NewClient(tUrl.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	code := status.Code(err)
	return code == 14 || code == 4 || code == 8 || code == 5 // Unavailable, DeadlineExceeded, ResourceExhausted, NotFound
}

func (c *tikvProxyClient) txn(f func(*kvTxn) error, retry int) (err error) {
	for i := 0; i <= retry; i++ {
		// Begin a new transaction by getting a start timestamp
		// For simplicity, we'll use current time as start timestamp
		// In a real implementation, this should come from PD

		proxyTxn := &tikvProxyTxn{
			client:  c.client,
			startTS: 0,
			writes:  make(map[string][]byte),
			retry:   i,
		}

		kvTx := &kvTxn{
			kvtxn: proxyTxn,
			retry: i,
		}

		err = f(kvTx)
		if err != nil {
			if c.shouldRetry(err) && i < retry {
				logger.Warnf("TiKV Proxy transaction failed (attempt %d/%d): %v", i+1, retry+1, err)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			return err
		}

		// Commit the transaction
		err = proxyTxn.commit()
		if err != nil {
			if c.shouldRetry(err) && i < retry {
				logger.Warnf("TiKV Proxy commit failed (attempt %d/%d): %v", i+1, retry+1, err)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			return err
		}

		return nil
	}
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
