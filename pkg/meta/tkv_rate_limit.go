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

package meta

import (
	"context"
	"reflect"
	"time"

	"golang.org/x/time/rate"
)

// Rate limit settings on the `kvtxn` interface (defined in pkg/meta/tkv.go) methods
// Field naming: <Method>OpQPS
type MetaKvTxnRateLimitConf struct {
	GetOpQPS    int `json:",omitempty"`
	GetsOpQPS   int `json:",omitempty"`
	ScanOpQPS   int `json:",omitempty"`
	ExistOpQPS  int `json:",omitempty"`
	SetOpQPS    int `json:",omitempty"`
	AppendOpQPS int `json:",omitempty"`
	IncrByOpQPS int `json:",omitempty"`
	DeleteOpQPS int `json:",omitempty"`
}

type rateLimitManager struct {
	conf     MetaKvTxnRateLimitConf
	limiters map[string]*rate.Limiter
}

var rateLimiter = &rateLimitManager{
	conf: MetaKvTxnRateLimitConf{0, 0, 0, 0, 0, 0, 0, 0},
	limiters: map[string]*rate.Limiter{
		"get":    nil,
		"gets":   nil,
		"scan":   nil,
		"exist":  nil,
		"set":    nil,
		"append": nil,
		"incrBy": nil,
		"delete": nil,
	},
}

func ReloadMetaKvTxnRateLimiter(new *MetaKvTxnRateLimitConf) {
	if new == nil {
		logger.Errorf("Init meta API rate limiter failed: configuration is nil")
		return
	}

	old := &rateLimiter.conf
	if reflect.DeepEqual(*old, *new) {
		logger.Debug("Skip reloading rate limit settings for meta kvtxn ops, no changes")
		return
	}

	logger.Infof("Reloading rate limit settings for meta kvtxn ops: %+v", new)

	if new.GetOpQPS != old.GetOpQPS {
		if new.GetOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["get"] = rate.NewLimiter(rate.Limit(float64(new.GetOpQPS)*0.85), new.GetOpQPS)
		}
	}
	if new.GetsOpQPS != old.GetsOpQPS {
		if new.GetsOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["gets"] = rate.NewLimiter(rate.Limit(float64(new.GetOpQPS)*0.85), new.GetsOpQPS)
		}
	}
	if new.ScanOpQPS != old.ScanOpQPS {
		if new.ScanOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["scan"] = rate.NewLimiter(rate.Limit(float64(new.GetOpQPS)*0.85), new.ScanOpQPS)
		}
	}
	if new.ExistOpQPS != old.ExistOpQPS {
		if new.ExistOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["exist"] = rate.NewLimiter(rate.Limit(float64(new.ExistOpQPS)*0.85), new.ExistOpQPS)
		}
	}
	if new.SetOpQPS != old.SetOpQPS {
		if new.SetOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["set"] = rate.NewLimiter(rate.Limit(float64(new.SetOpQPS)*0.85), new.SetOpQPS)
		}
	}
	if new.AppendOpQPS != old.AppendOpQPS {
		if new.AppendOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["append"] = rate.NewLimiter(rate.Limit(float64(new.AppendOpQPS)*0.85), new.AppendOpQPS)
		}
	}
	if new.IncrByOpQPS != old.IncrByOpQPS {
		if new.IncrByOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["incrBy"] = rate.NewLimiter(rate.Limit(float64(new.IncrByOpQPS)*0.85), new.IncrByOpQPS)
		}
	}
	if new.DeleteOpQPS != old.DeleteOpQPS {
		if new.DeleteOpQPS == 0 {
			rateLimiter.limiters["get"] = nil
		} else {
			rateLimiter.limiters["delete"] = rate.NewLimiter(rate.Limit(float64(new.DeleteOpQPS)*0.85), new.DeleteOpQPS)
		}
	}

	rateLimiter.conf = *new
}

func kvtxnRateLimit(method string) {
	limiter, ok := rateLimiter.limiters[method]
	if !ok {
		logger.Errorf("Rate limiter for kvtxn ops %s not found", method)
		return
	}

	if limiter == nil {
		logger.Debugf("No rate limiting for kvtxn ops %s", method)
		return
	}

	if allow := limiter.Allow(); !allow {
		logger.Debugf("kvtxn ops %s is rate limited", method)
		kvtxnRateLimitCount.WithLabelValues(method).Add(1)

		if err := limiter.Wait(context.Background()); err != nil {
			time.Sleep(10 * time.Millisecond)
			logger.Warnf("Rate limiter failed: method %s, error %v", method, err)
		}
	}
}
