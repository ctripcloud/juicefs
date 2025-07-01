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
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/juicedata/juicefs/pkg/metric"
	"github.com/juicedata/juicefs/pkg/proxy"
	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

func cmdTiKVProxy() *cli.Command {
	return &cli.Command{
		Name:      "tikv-proxy",
		Action:    tikvProxyAction,
		Category:  "SERVICE",
		Usage:     "Start a TiKV transaction proxy server",
		ArgsUsage: "TIKV-ADDRESS",
		Description: `
Start a gRPC server that provides a stateless proxy for TiKV transactions.
The proxy accepts transaction operations via gRPC and forwards them to TiKV cluster.

TIKV-ADDRESS is the address of the TiKV cluster (e.g., 127.0.0.1:2379).

Examples:
$ juicefs tikv-proxy 127.0.0.1:2379 --port 8080

Details: https://juicefs.com/docs/community/tikv_proxy`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log",
				Usage: "path for proxy log",
				Value: path.Join(getDefaultLogDir(), "juicefs-tikv-proxy.log"),
			},
			&cli.BoolFlag{
				Name:    "background",
				Aliases: []string{"d"},
				Usage:   "run in background",
			},
			&cli.StringFlag{
				Name:  "port",
				Usage: "port to listen on",
				Value: "8080",
			},
			&cli.StringFlag{
				Name:  "metrics",
				Usage: "address to expose metrics (e.g., 0.0.0.0:2112)",
			},
		},
	}
}

func proxyExposeMetrics(c *cli.Context, registerer prometheus.Registerer, registry *prometheus.Registry) string {
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

	proxyMetricsAddr := ln.Addr().String()
	logger.Infof("Prometheus metrics listening on %s", proxyMetricsAddr)
	return proxyMetricsAddr
}

func proxyWrapRegister(c *cli.Context) (*grpcprom.ServerMetrics, prometheus.Registerer, *prometheus.Registry) {
	commonLabels := prometheus.Labels{"juicefs_version": version.Version()}
	if h, err := os.Hostname(); err == nil {
		commonLabels["instance"] = h
	} else {
		logger.Warnf("cannot get hostname: %s", err)
	}
	// 创建Prometheus监控器
	srvMetrics := grpcprom.NewServerMetrics()

	registry := prometheus.NewRegistry()

	registerer := prometheus.WrapRegistererWithPrefix("tikv_proxy_", prometheus.WrapRegistererWith(commonLabels, registry))

	registerer.MustRegister(srvMetrics)
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())

	return srvMetrics, registerer, registry
}

func tikvProxyAction(c *cli.Context) error {
	setup(c, 1)

	if c.NArg() != 1 {
		return cli.ShowCommandHelp(c, "tikv-proxy")
	}

	tikvAddresses := c.Args().Get(0)
	proxyPort := c.String("port")
	localIP, err := getLocalIP()
	if err != nil {
		logger.Fatalf("Failed to get local IP: %v", err)
	}
	listenAddr := fmt.Sprintf("%s:%s", localIP, proxyPort)

	// Create TiKV proxy
	tikvProxy, err := proxy.NewTiKVProxy(tikvAddresses, listenAddr)
	if err != nil {
		logger.Fatalf("Failed to create TiKV proxy: %v", err)
	}
	defer tikvProxy.Close()
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY
		MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
		MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:                  5 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
	}

	// Wrap the default registry, all prometheus.MustRegister() calls should be afterwards
	srvMetrics, registerer, registry := proxyWrapRegister(c)

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			srvMetrics.StreamServerInterceptor(),
		),
	)
	proxyv1.RegisterTxnProxyServiceServer(grpcServer, tikvProxy)
	srvMetrics.InitializeMetrics(grpcServer)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	proxyExposeMetrics(c, registerer, registry)

	go func() {
		next := grpc_health_v1.HealthCheckResponse_SERVING
		for {
			err := tikvProxy.HealthCheck(context.Background())
			if err != nil {
				next = grpc_health_v1.HealthCheckResponse_NOT_SERVING
			} else {
				next = grpc_health_v1.HealthCheckResponse_SERVING
			}
			healthServer.SetServingStatus("", next)
			time.Sleep(3 * time.Second)
		}
	}()

	reflection.Register(grpcServer)

	// Listen on the specified address
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatalf("Failed to listen on %s: %v", listenAddr, err)
	}

	logger.Infof("TiKV Proxy server starting on %s", listenAddr)
	logger.Infof("Connected to TiKV cluster: %s", tikvAddresses)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server in a goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logger.Errorf("gRPC server error: %v", err)
			cancel()
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Infof("Received signal %v, shutting down gracefully...", sig)
	case <-ctx.Done():
		logger.Info("Context cancelled, shutting down...")
	}

	// Graceful shutdown
	logger.Info("Stopping gRPC server...")
	grpcServer.GracefulStop()
	logger.Info("TiKV Proxy server stopped")

	return nil
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// 检查是否为 IP 地址，并且不是回环地址（127.0.0.1）
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// 优先返回 IPv4 地址
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("无法获取本机 IP 地址")
}
