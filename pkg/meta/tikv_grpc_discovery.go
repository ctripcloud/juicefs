package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/resolver"
)

type proxyResponse struct {
	Proxy     string `json:"proxy"`
	ClusterID uint64 `json:"cluster_id"`
	StartTS   uint64 `json:"start_ts"`
}

var (
	activeProxyCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tikv_proxy_active_count",
		Help: "Number of active proxies",
	})
)

const (
	// DiscoveryScheme is the scheme for discovery resolver
	DiscoveryScheme = "tikv-proxy"
	// Default discovery interval
	DefaultDiscoveryInterval = 15 * time.Second
	// Max retry attempts for initial discovery
	MaxInitialRetryAttempts = 10
	// HTTP client timeout
	DefaultHTTPTimeout = 5 * time.Second
)

// Note: resolver registration is handled in tikv_tikv_proxy.go to avoid duplicate registration

// ServiceDiscovery 服务发现
type ServiceDiscovery struct {
	logger     *logrus.Entry
	httpClient *http.Client
}

// NewServiceDiscovery 新建发现服务
func NewServiceDiscovery() resolver.Builder {
	logger := logger.WithField("component", "grpc-discovery-builder")
	logger.Debugf("NewServiceDiscovery")

	// Create HTTP client with proper timeouts
	httpClient := &http.Client{
		Timeout: DefaultHTTPTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	return &ServiceDiscovery{
		logger:     logger,
		httpClient: httpClient,
	}
}

// Build 为给定目标创建一个新的`resolver`，当调用`grpc.Dial()`时执行
func (s *ServiceDiscovery) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	s.logger.Infof("Building resolver for target: %s", target.URL.Host)
	ctx, cancel := context.WithCancel(context.Background())

	r := &discoveryResolver{
		target:           target,
		cc:               cc,
		ctx:              ctx,
		cancel:           cancel,
		logger:           s.logger.WithField("target", target.URL.Host),
		httpClient:       s.httpClient,
		serverList:       make(map[string]resolver.Address),
		currentClusterID: 0,
		currentStartTS:   0,
	}

	// 获取初始服务列表，使用指数退避重试
	var lastErr error
	backoff := 100 * time.Millisecond
	for i := 0; i < MaxInitialRetryAttempts; i++ {
		if err := r.updateServiceList(); err != nil {
			lastErr = err
			r.logger.Debugf("Failed to get initial service list (attempt %d/%d): %v", i+1, MaxInitialRetryAttempts, err)
			if i < MaxInitialRetryAttempts-1 { // 最后一次不需要sleep
				time.Sleep(backoff)
				backoff = minDuration(backoff*2, 10*time.Second) // 指数退避，最大10秒
			}
		} else {
			r.logger.Infof("Successfully got initial service list on attempt %d, got proxies: %v", i+1, r.GetCurrentAddresses())
			lastErr = nil
			break
		}
	}

	if lastErr != nil {
		// 返回错误而不是Fatal退出
		return nil, fmt.Errorf("failed to get initial service list after %d attempts: %v", MaxInitialRetryAttempts, lastErr)
	}

	// 启动监听器
	go r.watcher()

	return r, nil
}

// Scheme return schema
func (s *ServiceDiscovery) Scheme() string {
	return DiscoveryScheme
}

// discoveryResolver 实现 resolver.Resolver 接口
type discoveryResolver struct {
	target     resolver.Target
	cc         resolver.ClientConn
	ctx        context.Context
	cancel     context.CancelFunc
	logger     *logrus.Entry
	httpClient *http.Client

	lock             sync.RWMutex
	serverList       map[string]resolver.Address // 服务列表
	currentClusterID uint64
	currentStartTS   uint64
	lastError        error
	errorCount       int
}

// ResolveNow 监视目标更新
func (r *discoveryResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.logger.Debug("ResolveNow called")
}

// Close 关闭
func (r *discoveryResolver) Close() {
	r.logger.Info("Closing discovery resolver")
	r.cancel()
}

// watcher 监听服务变化
func (r *discoveryResolver) watcher() {
	r.logger.Infof("Starting discovery watcher with interval %v", DefaultDiscoveryInterval)
	ticker := time.NewTicker(DefaultDiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("Discovery watcher stopped")
			return
		case <-ticker.C:
			if err := r.updateServiceList(); err != nil {
				r.lock.Lock()
				r.errorCount++
				r.lastError = err
				r.lock.Unlock()

				r.logger.Errorf("Failed to update service list (error count: %d): %v", r.errorCount, err)
			} else {
				r.lock.Lock()
				if r.errorCount > 0 {
					r.logger.Infof("Service discovery recovered after %d errors", r.errorCount)
					r.errorCount = 0
					r.lastError = nil
				}
				r.lock.Unlock()
			}
		}
	}
}

// updateServiceList 更新服务列表
func (r *discoveryResolver) updateServiceList() error {
	// 构建 discovery URL
	discoveryURL := r.getDiscoveryURL()

	// 获取服务列表, 如果服务器列表为空，返回error
	addrs, clusterID, startTS, err := r.fetchAddresses(discoveryURL)
	if err != nil {
		return fmt.Errorf("failed to fetch addresses: %v", err)
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// 检查是否需要更新
	shouldUpdate := false

	// 1. 检查集群ID变化（强制更新）
	if clusterID != r.currentClusterID {
		r.logger.Debugf("Cluster ID changed from %d to %d, forcing update", r.currentClusterID, clusterID)
		shouldUpdate = true
	}

	// 2. 检查 startTS（跳过较小的值）
	if startTS < r.currentStartTS {
		r.logger.Debugf("StartTS %d is smaller than current %d, skipping update", startTS, r.currentStartTS)
		return nil
	}

	// 3. 检查地址列表变化
	newServerList := make(map[string]resolver.Address)
	for i, addr := range addrs {
		key := fmt.Sprintf("proxy_%d", i)
		newServerList[key] = addr
	}

	if !r.serverListEqual(newServerList) {
		r.logger.Debugf("Server list changed")
		shouldUpdate = true
	}

	if !shouldUpdate {
		r.logger.Debug("No changes detected, skipping update")
		return nil
	}

	// 更新本地状态
	r.serverList = newServerList
	r.currentClusterID = clusterID
	r.currentStartTS = startTS

	// 通知 gRPC 更新地址列表
	addresses := r.getServices()
	state := resolver.State{
		Addresses: addresses,
	}

	activeProxyCount.Set(float64(len(addresses)))

	logger.Infof("update service list, clusterID: %d, startTS: %d, serverList: %v", clusterID, startTS, newServerList)
	if err := r.cc.UpdateState(state); err != nil {
		return fmt.Errorf("failed to update client connection state: %v", err)
	}
	return nil
}

// getDiscoveryURL 构建 discovery URL
func (r *discoveryResolver) getDiscoveryURL() string {
	scheme := "http"

	authority := r.target.URL.Host
	if authority == "" {
		authority = r.target.Endpoint()
	}

	path := r.target.URL.Path
	if path == "" {
		path = "discovery"
	}
	path = strings.TrimLeft(path, "/")

	return fmt.Sprintf("%s://%s/%s", scheme, authority, path)
}

// fetchAddresses 从 discovery 服务获取地址列表
func (r *discoveryResolver) fetchAddresses(discoveryURL string) ([]resolver.Address, uint64, uint64, error) {
	ctx, cancel := context.WithTimeout(r.ctx, DefaultHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", discoveryURL, nil)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to fetch discovery data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, 0, fmt.Errorf("discovery service returned status %d", resp.StatusCode)
	}

	var discoveryResp []proxyResponse
	if err := json.NewDecoder(resp.Body).Decode(&discoveryResp); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to decode discovery response: %v", err)
	}

	if len(discoveryResp) == 0 {
		return nil, 0, 0, fmt.Errorf("no proxies found in discovery response")
	}

	// 提取集群ID和startTS（从第一个响应）
	clusterID := discoveryResp[0].ClusterID
	startTS := discoveryResp[0].StartTS

	// 转换为 resolver.Address 列表
	var addrs []resolver.Address
	for _, proxy := range discoveryResp {
		addrs = append(addrs, resolver.Address{
			Addr: proxy.Proxy,
		})
	}

	// 排序以保证一致性
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Addr < addrs[j].Addr
	})

	return addrs, clusterID, startTS, nil
}

// serverListEqual 比较服务列表是否相等
func (r *discoveryResolver) serverListEqual(newList map[string]resolver.Address) bool {
	if len(r.serverList) != len(newList) {
		return false
	}

	for key, addr := range newList {
		if existing, ok := r.serverList[key]; !ok || existing.Addr != addr.Addr {
			return false
		}
	}

	return true
}

// getServices 获取服务地址列表
func (r *discoveryResolver) getServices() []resolver.Address {
	addrs := make([]resolver.Address, 0, len(r.serverList))
	for _, addr := range r.serverList {
		r.logger.Debugf("getServices: %s", addr.Addr)
		addrs = append(addrs, addr)
	}

	// 排序以保证一致性
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Addr < addrs[j].Addr
	})

	return addrs
}

// SetServiceList 新增服务地址（用于动态添加）
func (r *discoveryResolver) SetServiceList(key, addr string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.serverList[key] = resolver.Address{Addr: addr}

	// 通知 gRPC 更新
	state := resolver.State{
		Addresses: r.getServices(),
	}
	r.cc.UpdateState(state)

	r.logger.Debugf("Added service: key=%s, addr=%s", key, addr)
}

// DelServiceList 删除服务地址（用于动态删除）
func (r *discoveryResolver) DelServiceList(key string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, exists := r.serverList[key]; !exists {
		return
	}

	delete(r.serverList, key)

	// 通知 gRPC 更新
	state := resolver.State{
		Addresses: r.getServices(),
	}
	r.cc.UpdateState(state)

	r.logger.Infof("Removed service: key=%s", key)
}

// GetCurrentAddresses 获取当前地址列表（用于调试）
func (r *discoveryResolver) GetCurrentAddresses() []string {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var result []string
	for _, addr := range r.getServices() {
		result = append(result, addr.Addr)
	}
	return result
}

// GetCurrentClusterID 获取当前集群ID（用于调试）
func (r *discoveryResolver) GetCurrentClusterID() uint64 {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.currentClusterID
}

// Helper function
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// GetLastError 获取最后一次错误（用于调试）
func (r *discoveryResolver) GetLastError() error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.lastError
}

// GetErrorCount 获取错误计数（用于调试）
func (r *discoveryResolver) GetErrorCount() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.errorCount
}
