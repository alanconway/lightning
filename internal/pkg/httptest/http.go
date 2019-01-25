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

package httptest

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/alanconway/lightning/internal/pkg/test"
)

type HTTPServer struct {
	server   http.Server
	listener net.Listener
	Addr     string
}

func StartHTTPServer(t test.TB, h http.Handler) *HTTPServer {
	l, err := net.Listen("tcp", ":0")
	t.RequireNil(err)
	hs := &HTTPServer{
		server: http.Server{
			Handler:  h,
			ErrorLog: log.New(os.Stderr, "test/http[error]:", log.LstdFlags),
		},
		listener: l,
		Addr:     l.Addr().String(),
	}
	go hs.server.Serve(hs.listener)
	return hs
}

type HTTPRequest struct {
	*http.Request
	Body []byte
}

type HTTPChan chan *HTTPRequest

func (c HTTPChan) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	c <- &HTTPRequest{Request: r, Body: b}
}

func (hs *HTTPServer) Stop() { hs.server.Close() }
