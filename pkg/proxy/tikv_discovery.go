package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

type tProxy interface {
	GetAllProxies(ctx context.Context) (map[string]uint64, uint64, error)
	HealthCheck(ctx context.Context) error
	Close() error
}

type ProxyDiscovery struct {
	tikvProxy  tProxy
	proxyCache []string // active proxy addresses
	logger     *logrus.Entry
	proxyAddr  string
	ctx        context.Context
}

func NewProxyDiscovery(ctx context.Context, addr string, proxyAddr string) (*ProxyDiscovery, error) {
	tikvProxy, err := NewTiKVProxy(addr, "")
	if err != nil {
		return nil, err
	}

	pd := &ProxyDiscovery{
		tikvProxy:  tikvProxy,
		proxyCache: make([]string, 0),
		logger:     logger.WithField("component", "proxy-discovery"),
		proxyAddr:  proxyAddr,
		ctx:        ctx,
	}

	// Start background task
	go pd.updateProxies()

	return pd, nil
}

func (pd *ProxyDiscovery) Serve() error {
	server := &http.Server{
		Addr:    pd.proxyAddr,
		Handler: pd,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Infof("Received signal %v, shutting down gracefully...", sig)
	case <-pd.ctx.Done():
		logger.Info("Context cancelled, shutting down...")
	}

	// Graceful shutdown
	logger.Info("Stopping HTTP server...")
	if err := server.Shutdown(context.Background()); err != nil {
		logger.Errorf("Error during server shutdown: %v", err)
	}
	logger.Info("Proxy discovery server stopped")

	return nil
}

func (pd *ProxyDiscovery) Shutdown() error {
	return pd.tikvProxy.Close()
}

func (pd *ProxyDiscovery) updateProxies() {
	ticker := time.NewTicker(tikvProxySessionHeartbeatInterval)
	defer ticker.Stop()

	f := func() {
		proxies, _, err := pd.tikvProxy.GetAllProxies(pd.ctx)
		if err != nil {
			pd.logger.Errorf("Failed to get proxies: %v", err)
		}

		now := uint64(time.Now().Unix())
		activeProxies := make([]string, 0)

		// Only keep active proxies
		for proxy, activeTime := range proxies {
			logger.Infof("proxy %s is active, active time is %d, now is %d", proxy, activeTime, now)
			if activeTime >= now-uint64(tikvProxySessionHeartbeatTimeout.Seconds()) {
				activeProxies = append(activeProxies, proxy)
				logger.Infof("proxy %s is active", proxy)
			}
		}
		pd.proxyCache = activeProxies
		logger.Infof("update proxies, active proxies: %v", activeProxies)
	}

	f()
	for {
		select {
		case <-ticker.C:
			logger.Infof("update proxies ticker")
			f()
		case <-pd.ctx.Done():
			logger.Infof("update proxies context done")
			return
		}
	}
}

func (pd *ProxyDiscovery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/discovery":
		pd.handleDiscovery(w, r)
	case "/health":
		pd.handleHealth(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (pd *ProxyDiscovery) handleDiscovery(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pd.proxyCache)
}

func (pd *ProxyDiscovery) handleHealth(w http.ResponseWriter, _ *http.Request) {
	err := pd.tikvProxy.HealthCheck(context.Background())
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (pd *ProxyDiscovery) Close() error {
	return pd.tikvProxy.Close()
}
