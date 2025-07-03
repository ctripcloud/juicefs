package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	tikvProxyDiscoveryInterval = time.Second * 5
)

type tProxy interface {
	GetAllProxies(ctx context.Context) (map[string]uint64, uint64, error)
	GetClusterID() uint64
	HealthCheck(ctx context.Context) error
	Close() error
}

type proxyResponse struct {
	Proxy   string `json:"proxy"`
	ClusterID uint64 `json:"cluster_id"`
	StartTS   uint64 `json:"start_ts"`
}

type ProxyDiscovery struct {
	tikvProxy    tProxy
	proxyCache   []proxyResponse // active proxy addresses
	logger       *logrus.Entry
	proxyAddr    string
	ctx          context.Context
	shutdownOnce sync.Once
}

var (
	tikvProxyDiscoveryAliveMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tikv_proxy_discovery_alive",
		Help: "The number of alive tikv proxies.",
	})
	tikvProxyDiscoveryErrorCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tikv_proxy_discovery_error_count",
		Help: "The number of errors in tikv proxy discovery.",
	})
	tikvProxyDiscoveryLastCheckTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tikv_proxy_discovery_last_check_time",
		Help: "The last time tikv proxy discovery was checked.",
	})
	tikvProxyDiscoveryRequestCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tikv_proxy_discovery_request_count",
		Help: "The number of requests to tikv proxy discovery.",
	}, []string{"method"})
)

func InitTikvProxyDiscoveryMetrics(reg prometheus.Registerer) {
	if reg != nil {
		reg.MustRegister(tikvProxyDiscoveryAliveMetric)
		reg.MustRegister(tikvProxyDiscoveryErrorCount)
		reg.MustRegister(tikvProxyDiscoveryLastCheckTime)
		reg.MustRegister(tikvProxyDiscoveryRequestCount)
	}
}

func NewProxyDiscovery(ctx context.Context, addr string, proxyAddr string) (*ProxyDiscovery, error) {
	tikvProxy, err := NewTiKVProxy(addr, "")
	if err != nil {
		return nil, err
	}

	logger.Infof("NewProxyDiscovery, addr: %s, proxyAddr: %s", addr, proxyAddr)
	pd := &ProxyDiscovery{
		tikvProxy:  tikvProxy,
		proxyCache: make([]proxyResponse, 0),
		proxyAddr:  proxyAddr,
		logger:     logger.WithField("component", "proxy-discovery"),
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
	var err error
	pd.shutdownOnce.Do(func() {
		if pd.tikvProxy != nil {
			err = pd.tikvProxy.Close()
		}
	})
	return err
}

func (pd *ProxyDiscovery) updateProxies() {
	ticker := time.NewTicker(tikvProxyDiscoveryInterval)
	defer ticker.Stop()

	f := func() {
		proxies, startTS, err := pd.tikvProxy.GetAllProxies(pd.ctx)
		if err != nil {
			pd.logger.Errorf("Failed to get proxies: %v", err)
		}

		now := uint64(time.Now().Unix())
		activeProxies := make([]proxyResponse, 0)

		// Only keep active proxies
		for proxy, activeTime := range proxies {
			logger.Debugf("proxy %s active time is %d, now is %d", proxy, activeTime, now)
			if activeTime >= now-uint64(tikvProxySessionHeartbeatTimeout.Seconds()) {
				activeProxies = append(activeProxies, proxyResponse{
					Proxy:     proxy,
					ClusterID: pd.tikvProxy.GetClusterID(),
					StartTS:   startTS,
				})
				logger.Debugf("proxy %s is active", proxy)
			}
		}
		pd.proxyCache = activeProxies
		tikvProxyDiscoveryAliveMetric.Set(float64(len(activeProxies)))
		tikvProxyDiscoveryLastCheckTime.Set(float64(now))
		logger.Debugf("update proxies, active proxies: %v", activeProxies)
	}

	for {
		f()
		select {
		case <-ticker.C:
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
		tikvProxyDiscoveryRequestCount.WithLabelValues("discovery").Inc()
	case "/health":
		pd.handleHealth(w, r)
		tikvProxyDiscoveryRequestCount.WithLabelValues("health").Inc()
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
	var err error
	pd.shutdownOnce.Do(func() {
		if pd.tikvProxy != nil {
			err = pd.tikvProxy.Close()
		}
	})
	return err
}
