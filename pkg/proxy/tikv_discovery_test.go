package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProxyDiscovery_Integration performs an integration test on the proxy discovery service
// using a real TiKV backend. This test requires a running TiKV instance at the address
// specified in `FatTiKVAddr`.
func TestProxyDiscovery_Integration(t *testing.T) {

	
	// start a tikv proxy in localhost:8079
	tikvProxy1, err := NewTiKVProxy(FatTiKVAddr, "localhost:8079")
	require.NoError(t, err)

	// start a tikv proxy in localhost:8080
	tikvProxy2, err := NewTiKVProxy(FatTiKVAddr, "localhost:8080")
	require.NoError(t, err)
	defer tikvProxy2.Close()

	// Create a discovery service instance, which will also register itself as a proxy
	// at localhost:8081.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	discoveryService, err := NewProxyDiscovery(ctx, FatTiKVAddr, "localhost:8081")
	go discoveryService.Serve()
	require.NoError(t, err, "Failed to create primary discovery service")
	require.NotNil(t, discoveryService)
	defer discoveryService.Close()

	// Create a second proxy instance to be discovered. It will register at localhost:8082.
	// We only need it for its registration side-effect.
	otherProxy, err := NewProxyDiscovery(ctx, FatTiKVAddr, "localhost:8082")
	require.NoError(t, err, "Failed to create second proxy instance")
	require.NotNil(t, otherProxy)
	defer otherProxy.Close()

	// --- Test /health endpoint ---
	t.Run("HealthCheck", func(t *testing.T) {
		healthResp, err := http.Get("http://localhost:8081/health")
		require.NoError(t, err)
		defer healthResp.Body.Close()
		assert.Equal(t, http.StatusOK, healthResp.StatusCode, "Health check should be OK")

		var healthStatus map[string]string
		err = json.NewDecoder(healthResp.Body).Decode(&healthStatus)
		require.NoError(t, err)
		assert.Equal(t, "healthy", healthStatus["status"])
	})

	// --- Test /discovery endpoint ---
	t.Run("Discovery", func(t *testing.T) {
		discoveryResp, err := http.Get("http://localhost:8081/discovery")
		require.NoError(t, err)
		defer discoveryResp.Body.Close()
		assert.Equal(t, http.StatusOK, discoveryResp.StatusCode, "Discovery request should be OK")

		var discoveredProxies []string
		err = json.NewDecoder(discoveryResp.Body).Decode(&discoveredProxies)
		require.NoError(t, err, "Failed to decode discovery response")

		// We expect to find both our service and the other proxy.
		expectedHasProxy := []string{"localhost:8079", "localhost:8080"}
		sort.Strings(discoveredProxies)
		sort.Strings(expectedHasProxy)
		for _, proxy := range expectedHasProxy {
			assert.Contains(t, discoveredProxies, proxy, "Should discover all active proxies")
		}
		t.Logf("Successfully discovered proxies: %v", discoveredProxies)
	})

	// --- Test default handler for unknown paths ---
	t.Run("NotFound", func(t *testing.T) {
		notFoundResp, err := http.Get("http://localhost:8081/unknown-path")
		require.NoError(t, err)
		defer notFoundResp.Body.Close()
		assert.Equal(t, http.StatusNotFound, notFoundResp.StatusCode)
	})

	// --- Test proxy discovery after tikvProxy1 is closed ---
	t.Run("ProxyDiscoveryAfterTiKVProxy1IsClosed", func(t *testing.T) {
		discoveryResp, err := http.Get("http://localhost:8081/discovery")
		require.NoError(t, err)
		defer discoveryResp.Body.Close()
		assert.Equal(t, http.StatusOK, discoveryResp.StatusCode, "Discovery request should be OK")

		var discoveredProxies []string
		err = json.NewDecoder(discoveryResp.Body).Decode(&discoveredProxies)
		require.NoError(t, err, "Failed to decode discovery response")

		expectedHasProxy := []string{"localhost:8079", "localhost:8080"}
		sort.Strings(discoveredProxies)
		sort.Strings(expectedHasProxy)
		for _, proxy := range expectedHasProxy {
			assert.Contains(t, discoveredProxies, proxy, "Should discover all active proxies")
		}
		t.Logf("Successfully discovered proxies: %v", discoveredProxies)

		// close tikvProxy1
		t.Log("close tikvProxy1")
		err = tikvProxy1.Close()
		require.NoError(t, err)

		// wait for 20 seconds
		time.Sleep(20 * time.Second)

		discoveryResp, err = http.Get("http://localhost:8081/discovery")
		require.NoError(t, err)
		defer discoveryResp.Body.Close()
		assert.Equal(t, http.StatusOK, discoveryResp.StatusCode, "Discovery request should be OK")

		var discoveredProxies2 []string
		err = json.NewDecoder(discoveryResp.Body).Decode(&discoveredProxies2)
		require.NoError(t, err, "Failed to decode discovery response")

		expectedNotHasProxy := []string{"localhost:8079"}
		sort.Strings(discoveredProxies2)
		sort.Strings(expectedNotHasProxy)
		for _, proxy := range expectedNotHasProxy {
			assert.NotContains(t, discoveredProxies2, proxy, "Should not discover inactive proxies")
		}
		t.Logf("Successfully discovered proxies: %v", discoveredProxies)
	})

}
