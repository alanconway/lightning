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
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

// Source is a HTTP server that converts requests to cloud-events.
type Source struct {
	// HTTP server settings. Do not modify Server.Handler
	Server http.Server

	listener  net.Listener
	log       *zap.Logger
	incoming  chan lightning.Message
	closeOnce sync.Once
	busy      sync.WaitGroup
	err       lightning.AtomicError
}

func NewSource(capacity int, log *zap.Logger) *Source {
	s := &Source{
		log:      log.Named(lightning.UniqueID("http-source")),
		incoming: make(chan lightning.Message, capacity),
	}
	s.Server.Handler = http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			s.log.Debug("received", zap.Any("url", req.URL), zap.Any("headers", req.Header))
			if m, err := NewMessage(req); err != nil {
				s.err.Set(err)
				s.Close()
			} else {
				s.incoming <- m
			}
		})
	s.Server.ErrorLog, _ = zap.NewStdLogAt(s.log, zap.ErrorLevel)
	s.busy.Add(1) // Removed in Close()
	return s
}

func (s *Source) Receive() (m lightning.Message, err error) {
	defer func() {
		if err != nil {
			s.log.Error("receive failed", zap.Error(err))
		}
	}()
	if m, ok := <-s.incoming; ok {
		return m, nil
	} else {
		return nil, s.err.Get()
	}
}

func (s *Source) Close() {
	s.log.Debug("closing", zap.Error(s.err.Get()))
	s.Server.Shutdown(nil)
	s.err.Set(io.EOF)
	s.busy.Done()
}

func (s *Source) Wait() error { s.busy.Wait(); return s.err.Get() }

// Start serving a listener, returns immediately.
func (s *Source) Start(l net.Listener) {
	s.log.Debug("listening", zap.Any("address", l.Addr()))
	s.busy.Add(1)
	go func() { defer s.busy.Done(); s.err.Set(s.Server.Serve(l)) }()
}
