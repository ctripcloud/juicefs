/*
 * JuiceFS, Copyright 2024 Juicedata, Inc.
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
	"fmt"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/meta"
)

const (
	metaKvTxnRateLimitsUsage = "limit the client's meta kvtxn ops (get|gets|scan|exist|set|append|incrBy|delete). " +
		"Example: 'get:1000,gets:1000,scan:1000', 0 means no limit, default no limit"
)

// rateLimits string format: 'get:10000,gets:10000,dels:2000'
func parseMetaKvTxnRateLimits(rateLimits string) (*meta.MetaKvTxnRateLimitConf, error) {
	if rateLimits == "" {
		return nil, nil
	}

	limits := &meta.MetaKvTxnRateLimitConf{}
	items := strings.Split(rateLimits, ",")
	for _, s := range items {
		qpsSetting := strings.Split(s, ":")
		if len(qpsSetting) != 2 {
			return nil, fmt.Errorf("unrecognized kvtxn rate limiting config %s", s)
		}

		method, qpsStr := qpsSetting[0], qpsSetting[1]
		if method == "" {
			return nil, fmt.Errorf("meta method can't be empty: %s", qpsSetting)
		}

		qps, err := strconv.Atoi(qpsStr)
		if err != nil {
			return nil, fmt.Errorf("convert rate limit value %s to int failed: %v", qpsStr, err)
		}

		logger.Debugf("Parsing meta kvtxn rate limit configuration: method %s max QPS %s", method, qps)

		switch method {
		case "get":
			limits.GetOpQPS = qps
		case "gets":
			limits.GetsOpQPS = qps
		case "scan":
			limits.ScanOpQPS = qps
		case "exist":
			limits.ExistOpQPS = qps
		case "set":
			limits.SetOpQPS = qps
		case "append":
			limits.AppendOpQPS = qps
		case "incrBy":
			limits.IncrByOpQPS = qps
		case "delete":
			limits.DeleteOpQPS = qps
		default:
			return nil, fmt.Errorf("unsupported meta kvtxn ops %s for rate limiting", method)
		}
	}

	return limits, nil
}
