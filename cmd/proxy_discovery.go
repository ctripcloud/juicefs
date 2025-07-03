/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/juicedata/juicefs/pkg/metric"
	"github.com/juicedata/juicefs/pkg/proxy"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

var (
	discoveryHealthMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxy_discovery_health",
		Help: "Health status of the proxy discovery service",
	})
)

func cmdProxyDiscovery() *cli.Command {
	return &cli.Command{
		Name:      "proxy-discovery",
		Action:    proxyDiscoveryAction,
		Category:  "SERVICE",
		Usage:     "Start a proxy discovery server",
		ArgsUsage: "TIKV-ADDRESS",
		Description: `
Start an HTTP server that provides discovery service for TiKV proxies.
The server accepts HTTP requests and returns active proxy addresses.

TIKV-ADDRESS is the address of TiKV cluster (e.g., 127.0.0.1:2379).

Examples:
$ juicefs proxy-discovery 127.0.0.1:2379 127.0.0.1:8080 --ip 127.0.0.1 --port 8081
$ juicefs proxy-discovery 127.0.0.1:2379,127.0.0.1:2380 127.0.0.1:8080 --port 8081`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log",
				Usage: "path for discovery log",
				Value: path.Join(getDefaultLogDir(), "juicefs-proxy-discovery.log"),
			},
			&cli.StringFlag{
				Name:  "port",
				Usage: "port to listen on",
				Value: "8080",
			},
			&cli.StringFlag{
				Name:  "metrics",
				Usage: "address to expose metrics (e.g., 127.0.0.1:33900)",
				Value: "127.0.0.1:33900",
			},
		},
	}
}

// Reuse metrics exposure code from tikv_proxy.go
func discoveryExposeMetrics(c *cli.Context, registerer prometheus.Registerer, registry *prometheus.Registry) string {
	var ip, port string
	var err error

	if c.IsSet("metrics") {
		metricsAddr := c.String("metrics")
		ip, port, err = net.SplitHostPort(metricsAddr)
		if err != nil {
			logger.Fatalf("Invalid format for --metrics flag '%s': %v", metricsAddr, err)
		}
	} else {
		ip, err = getLocalIP()
		if err != nil {
			logger.Errorf("Get local ip failed, use 0.0.0.0 as fallback: %v", err)
			ip = "0.0.0.0"
		}
		port = "0"
	}

	go metric.UpdateMetrics(registerer)

	// 创建并注册 BuildInfoCollector
	registerer.MustRegister(collectors.NewBuildInfoCollector())

	// 设置HTTP处理器来暴露指标
	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			EnableOpenMetrics: false,
		},
	))

	// 尝试在指定的IP和端口上监听
	listenAddr := net.JoinHostPort(ip, port)
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		if c.IsSet("metrics") {
			logger.Errorf("Listen on metrics address %s failed: %v", listenAddr, err)
			return ""
		}
		logger.Errorf("Listen on auto-assigned metrics address failed: %v", err)
		return ""
	}

	// 启动HTTP服务器来处理指标请求
	go func() {
		if err := http.Serve(ln, nil); err != nil {
			logger.Errorf("Metrics server failed: %s", err)
		}
	}()

	discoveryMetricsAddr := ln.Addr().String()
	logger.Infof("Prometheus metrics listening on %s", discoveryMetricsAddr)
	return discoveryMetricsAddr
}

// Create registerer for proxy discovery service (no gRPC metrics needed)
func discoveryWrapRegister(c *cli.Context) (prometheus.Registerer, *prometheus.Registry) {
	commonLabels := prometheus.Labels{"juicefs_version": version.Version()}
	if h, err := os.Hostname(); err == nil {
		commonLabels["instance"] = h
	} else {
		logger.Warnf("cannot get hostname: %s", err)
	}

	registry := prometheus.NewRegistry()

	registerer := prometheus.WrapRegistererWithPrefix("proxy_discovery_", prometheus.WrapRegistererWith(commonLabels, registry))

	registerer.MustRegister(discoveryHealthMetric)
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())

	return registerer, registry
}

func proxyDiscoveryAction(c *cli.Context) error {
	setup(c, 1)

	if c.NArg() != 1 {
		return cli.ShowCommandHelp(c, "proxy-discovery")
	}

	tikvAddr := c.Args().Get(0)
	proxyPort := c.String("port")
	localIP, err := getLocalIP()
	if err != nil {
		logger.Fatalf("Failed to get local IP: %v", err)
	}
	listenAddr := fmt.Sprintf("%s:%s", localIP, proxyPort)

	// Setup metrics
	registerer, registry := discoveryWrapRegister(c)
	discoveryExposeMetrics(c, registerer, registry)

	// Create proxy discovery service
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd, err := proxy.NewProxyDiscovery(ctx, tikvAddr, listenAddr)

	if err != nil {
		time.Sleep(time.Second * 10)
		logger.Fatalf("Failed to create proxy discovery: %v", err)
	}
	proxy.InitTikvProxyDiscoveryMetrics(registerer)
	defer pd.Shutdown()

	// Set initial health status
	discoveryHealthMetric.Set(1)

	logger.Infof("Proxy discovery server starting on %s", listenAddr)
	logger.Infof("Connected to TiKV cluster: %s", tikvAddr)

	return pd.Serve()
}
