package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
)

var (
	FatTiKVAddr  = "10.119.120.228:2379"
	FatProxyAddr = "localhost:8080"
)

// TestInterfaceCompliance is a compile-time check that ensures TiKVProxy
// implements the TxnProxyServiceServer interface.
func TestInterfaceCompliance(t *testing.T) {
	var _ proxyv1.TxnProxyServiceServer = (*TiKVProxy)(nil)
}

func TestTiKVProxyCreate(t *testing.T) {
	tikvProxy, err := NewTiKVProxy(FatTiKVAddr, FatProxyAddr)
	if err != nil {
		t.Fatalf("create tikv proxy failed: %v", err)
	}
	txn, err := tikvProxy.client.Begin()
	if err != nil {
		t.Fatalf("begin txn failed: %v", err)
	}
	startTS := txn.StartTS()
	assert.NotZero(t, startTS)
	clusterID := tikvProxy.client.GetClusterID()
	t.Logf("clusterID: %d", clusterID)
	assert.NotZero(t, clusterID)
	tikvProxy.Close()
}


func TestGetAllProxies(t *testing.T) {
	tikvProxy, _ := NewTiKVProxy(FatTiKVAddr, FatProxyAddr)
	proxies, startTS, err := tikvProxy.GetAllProxies(context.Background())
	if err != nil {
		t.Fatalf("get all proxies failed: %v", err)
	}
	t.Logf("proxies: %v, startTS: %d", proxies, startTS)
	if len(proxies) == 0 {
		t.Fatalf("no proxies found, it should have at least one proxy")
	}

	localProxyValue, err := tikvProxy.Get(context.Background(), &proxyv1.GetRequest{
		Key: []byte(fmt.Sprintf("%s%s", tikvProxySessionKeyPrefix, FatProxyAddr)),
	})
	if err != nil {
		t.Fatalf("get local proxy value failed: %v", err)
	}
	t.Logf("local proxy value: %v", localProxyValue)
	tikvProxy.Close()
}
