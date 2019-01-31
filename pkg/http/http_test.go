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
	"net/http"
	"net/url"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/httptest"
	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

type testMsg struct {
	e        lightning.Event
	finished bool
	err      error
}

func (m *testMsg) Event() (lightning.Event, error) { return m.e, nil }

func (m *testMsg) Structured() *lightning.Structured {
	if s, err := m.e.Format(lightning.JSONFormat); err == nil {
		return s
	} else {
		return nil
	}
}

func TestSink(tt *testing.T) {
	t := test.New(tt)

	hs := httptest.NewServer()
	defer hs.Close()

	s := &Sink{URL: &url.URL{Scheme: "http", Host: hs.Addr}, Client: &http.Client{}, Log: zap.NewNop()}
	go s.Send(&testMsg{e: lightning.Event{"specversion": "2.0", "data": "hello"}})

	got := <-hs.Incoming
	t.ExpectEqual(lightning.JSONFormat.Name(), got.Header.Get("Content-Type"))
	t.ExpectEqual(`{"data":"hello","specversion":"2.0"}`, string(got.Body))
}
