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
	"net"
	"net/http"
)

type Request struct {
	*http.Request
	Body []byte // Save body bytes
}

type Server struct {
	http.Server
	Addr     string
	Incoming chan *Request
}

func NewServer() *Server {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	s := &Server{
		Addr:     l.Addr().String(),
		Incoming: make(chan *Request),
	}
	s.Server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		s.Incoming <- &Request{Request: r, Body: b}
	})
	go s.Serve(l)
	return s
}
