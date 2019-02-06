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
	"net/url"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

func TestSinkSource(tt *testing.T) {
	t := test.New(tt)
	logger, _ := zap.NewDevelopment()

	// Client sink connects to server source.
	l, err := net.Listen("tcp", ":0")
	t.RequireNil(err)
	defer l.Close()
	source := NewSource(1, logger)
	source.Start(l)
	defer source.Close()

	sink := NewSink(&url.URL{Scheme: "http", Host: l.Addr().String()}, nil, logger)
	defer sink.Close()

	// Binary message
	e := lightning.Event{"specversion": "0.2", "data": []byte("binary"), "contenttype": "text/plain"}
	sink.Send(e)
	got, err := source.Receive()
	t.RequireNil(err)
	sm := got.Structured()
	t.Expectf(sm == nil, "unexpected structured message: %#v", sm)
	if e2, err := got.Event(); err != nil {
		t.Error(err)
	} else {
		t.ExpectEqual(e, e2)
	}

	// Structured message
	bytes := []byte(`{"data":"structured","specversion":"0.2","contenttype":"text/plain"}`)
	sm = &lightning.Structured{Format: lightning.JSONFormat, Bytes: bytes}
	t.RequireNil(sink.Send(sm))
	if got, err = source.Receive(); err != nil {
		t.Error(err)
	} else {
		sm2 := got.Structured()
		t.Requiref(sm != nil, "unexpected structured message: %#v", sm)
		t.ExpectEqual(lightning.JSONFormat.Name(), sm2.Format.Name())
		t.ExpectEqual(string(bytes), string(sm2.Bytes))
	}
}
