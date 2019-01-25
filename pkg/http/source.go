/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package http

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

// Source is a HTTP server that generates cloud-events.
type Source struct {
	// HTTP server settings.
	// Do not set Server.Handler, it will be set by the Source
	Server http.Server

	// Listeners to serve. If empty, Run() will call http.Server.ListenAndServe()
	Listeners []net.Listener

	// Log for source log messages
	Log *zap.Logger

	incoming  chan lightning.Message
	done      chan struct{}
	closeOnce sync.Once
	err       error
}

func (s *Source) Incoming() <-chan lightning.Message { return s.incoming }

func (s *Source) handler(w http.ResponseWriter, r *http.Request) {
	m := Message{Req: r}
	s.incoming <- m
	// Always respond 200 OK. For QoS > 0 we need to block here
	// for the value of m.Finish()
}

func (s *Source) Run() error {
	if s.Server.Handler != nil {
		panic("Source.Server.Handler must not be set")
	}
	s.Server.Handler = http.HandlerFunc(s.handler)
	if s.Server.ErrorLog == nil {
		s.Server.ErrorLog, _ = zap.NewStdLogAt(s.Log, zap.ErrorLevel)
	}

	if len(s.Listeners) == 0 {
		return s.Server.ListenAndServe()
	} else {
		var wait sync.WaitGroup
		var atomicErr atomic.Value
		wait.Add(len(s.Listeners))
		for _, l := range s.Listeners {
			go func(l net.Listener) {
				if err := s.Server.Serve(l); err != nil {
					atomicErr.Store(err)
				}
				wait.Done()
			}(l)
		}
		wait.Wait()
		err, _ := atomicErr.Load().(error)
		return err
	}
}

// Close calls Server.Shutdown() which waits for all requests to complete.
func (s *Source) Close() { s.Server.Shutdown(nil) }
