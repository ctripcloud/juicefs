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
	"path"

	"github.com/juicedata/juicefs/pkg/proxy"
	"github.com/urfave/cli/v2"
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
		},
	}
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

	// Create proxy discovery service
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd, err := proxy.NewProxyDiscovery(ctx, tikvAddr, listenAddr)
	if err != nil {
		logger.Fatalf("Failed to create proxy discovery: %v", err)
	}
	defer pd.Shutdown()

	return pd.Serve()
}
