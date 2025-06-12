package proxy

import (
	"testing"

	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
)

// TestInterfaceCompliance is a compile-time check that ensures TiKVProxy
// implements the TxnProxyServiceServer interface.
func TestInterfaceCompliance(t *testing.T) {
	var _ proxyv1.TxnProxyServiceServer = (*TiKVProxy)(nil)
}
