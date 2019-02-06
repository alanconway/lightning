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

package amqp

import (
	"net/url"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	// Change this to zap.NewDevelopment() for debugging
	logger = zap.NewNop()
}

func textEvent(data string) lightning.Event {
	return lightning.Event{"specversion": "0.2", "data": data, "foo": "bar"}
}

// No contenttype, "native" encoding of number - float64 is portable for JSON
func numberEvent(number float64) lightning.Event {
	return lightning.Event{"specversion": "0.2", "data": number, "foo": "bar"}
}

func structured(e lightning.Event) *lightning.Structured {
	if s, err := e.StructuredAs(lightning.JSONFormat); err == nil {
		return s
	} else {
		panic(err)
	}
}

func matchBinary(t test.TB, want lightning.Event, got lightning.Message) {
	t.Helper()
	t.Expect(got.Structured() == nil, "expected binary, got structured")
	if e, err := got.Event(); err != nil {
		t.Error(err)
	} else {
		t.ExpectEqual(want, e)
	}
}

func matchStructured(t test.TB, want lightning.Event, got lightning.Message) {
	t.Helper()
	if wantBytes, err := lightning.JSONFormat.Marshal(want); err != nil {
		t.Error(err)
	} else if s := got.Structured(); s == nil {
		t.Error("not structured")
	} else {
		t.ExpectEqual(string(wantBytes), string(s.Bytes))
	}
}

func TestClientSinkServerSource(tt *testing.T) {
	t := test.New(tt)

	source, err := NewServerSource("tcp", ":0", 10, logger)
	t.RequireNil(err)
	defer source.Close()
	t.RequireEqual(1, len(source.Listeners()))

	u := url.URL{Host: source.Listeners()[0].Addr().String(), Path: "client-sink"}
	sink, err := NewClientSink(&u, 10, logger)
	t.RequireNil(err)
	defer sink.Close()

	messages := []lightning.Message{
		textEvent("binary"), numberEvent(42),
		structured(textEvent("structured")), structured(numberEvent(13)),
	}
	for _, m := range messages {
		t.RequireNil(sink.Send(m))
	}
	m, err := source.Receive()
	t.RequireNil(err)
	matchBinary(t, textEvent("binary"), m)

	m, err = source.Receive()
	t.RequireNil(err)
	matchBinary(t, numberEvent(42), m)

	m, err = source.Receive()
	t.RequireNil(err)
	matchStructured(t, textEvent("structured"), m)

	m, err = source.Receive()
	t.RequireNil(err)
	matchStructured(t, numberEvent(13), m)
}

func TestClientSourceServerSink(tt *testing.T) {
	t := test.New(tt)

	sink, err := NewServerSink("tcp", ":0", 10, logger)
	t.RequireNil(err)
	t.RequireEqual(1, len(sink.Listeners()))

	u := url.URL{Host: sink.Listeners()[0].Addr().String(), Path: "client-source"}
	source, err := NewClientSource(&u, 10, logger)
	t.RequireNil(err)

	messages := []lightning.Message{
		textEvent("binary"), numberEvent(42),
		structured(textEvent("structured")), structured(numberEvent(13)),
	}
	for _, m := range messages {
		t.RequireNil(sink.Send(m))
	}
	m, err := source.Receive()
	t.RequireNil(err)
	matchBinary(t, textEvent("binary"), m)

	m, err = source.Receive()
	t.RequireNil(err)
	matchBinary(t, numberEvent(42), m)

	m, err = source.Receive()
	t.RequireNil(err)
	matchStructured(t, textEvent("structured"), m)

	m, err = source.Receive()
	t.RequireNil(err)
	matchStructured(t, numberEvent(13), m)
}
