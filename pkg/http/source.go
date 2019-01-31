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

// ServerSource is a HTTP server that converts requests to cloud-events.
type ServerSource struct {
	// HTTP server settings. Do not modify Server.Handler
	Server http.Server

	log       *zap.Logger
	incoming  chan lightning.Message
	closeOnce sync.Once
	busy      sync.WaitGroup
	err       lightning.AtomicError
}

func NewServerSource(log *zap.Logger) *ServerSource {
	s := &ServerSource{
		log:      log,
		incoming: make(chan lightning.Message),
	}
	s.Server.Handler = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { s.incoming <- Message{Req: r} })
	s.Server.ErrorLog, _ = zap.NewStdLogAt(s.log, zap.ErrorLevel)
	s.busy.Add(1) // Removed in Close()
	return s
}

func (s *ServerSource) Receive() (lightning.Message, error) {
	if m, ok := <-s.incoming; ok {
		return m, nil
	} else {
		return nil, s.err.Get()
	}
}

func (s *ServerSource) Close()      { s.Server.Shutdown(nil); s.err.Set(io.EOF); s.busy.Done() }
func (s *ServerSource) Wait() error { s.busy.Wait(); return s.err.Get() }

// Start listening to a listener
func (s *ServerSource) Start(l net.Listener) {
	s.busy.Add(1)
	go func() { s.err.Set(s.Server.Serve(l)); s.busy.Done() }()
}
