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
	"net"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/juicedata/juicefs/pkg/proxy"
	proxyv1 "github.com/juicedata/juicefs/pkg/proxy/v1"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"
    "google.golang.org/grpc/health/grpc_health_v1"
)

func cmdTiKVProxy() *cli.Command {
	return &cli.Command{
		Name:      "tikv-proxy",
		Action:    tikvProxyAction,
		Category:  "SERVICE",
		Usage:     "Start a TiKV transaction proxy server",
		ArgsUsage: "LISTEN-ADDRESS",
		Description: `
Start a gRPC server that provides a stateless proxy for TiKV transactions.
The proxy accepts transaction operations via gRPC and forwards them to TiKV cluster.

LISTEN-ADDRESS is the address where the gRPC server will listen (e.g., :8080).

Examples:
$ juicefs tikv-proxy --tikv 127.0.0.1:2379 :8080
$ juicefs tikv-proxy --tikv 127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381 0.0.0.0:8080

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
		},
	}
}

func tikvProxyAction(c *cli.Context) error {
	setup(c, 2)

	if c.NArg() != 2 {
		return cli.ShowCommandHelp(c, "tikv-proxy")
	}

	tikvAddresses := c.Args().Get(0)
	listenAddr := c.Args().Get(1)

	// Create TiKV proxy
	tikvProxy, err := proxy.NewTiKVProxy(tikvAddresses)
	if err != nil {
		logger.Fatalf("Failed to create TiKV proxy: %v", err)
	}
	defer tikvProxy.Close()

	// Create gRPC server
	grpcServer := grpc.NewServer()
	proxyv1.RegisterTxnProxyServiceServer(grpcServer, tikvProxy)

    healthServer := health.NewServer()
    healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
    grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

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
