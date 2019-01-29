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
	"testing"

	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	"qpid.apache.org/amqp"
)

func TestBinaryNative(tt *testing.T) {
	t := test.New(tt)
	e := lightning.Event{"specversion": "2.0", "id": "foo", "data": "hello"}
	am, err := NewBinary(e)
	t.RequireNil(err)
	t.ExpectEqual(
		map[string]interface{}{"cloudevents:specversion": "2.0", "cloudevents:id": "foo"},
		am.ApplicationProperties())
	t.ExpectEqual("hello", am.Body())

	m := Message{AMQP: am}
	s := m.Structured()
	t.Expect(s == nil, "expect not structured")
	e, err = m.Event()
	t.ExpectNil(err)
	t.ExpectEqual(lightning.Event{"id": "foo", "specversion": "2.0", "data": "hello"}, e)
}

func TestBinaryContentType(tt *testing.T) {
	t := test.New(tt)
	e := lightning.Event{"specversion": "2.0", "contenttype": "text/plain", "data": []byte("hello")}
	am, err := NewBinary(e)
	t.RequireNil(err)
	t.ExpectEqual("text/plain", am.ContentType())
	t.ExpectEqual(
		map[string]interface{}{"cloudevents:specversion": "2.0"},
		am.ApplicationProperties())
	t.ExpectEqual(amqp.Binary("hello"), am.Body())

	m := Message{AMQP: am}
	s := m.Structured()
	t.Expect(s == nil, "expect not structured")
	e, err = m.Event()
	t.ExpectNil(err)
	e.DataValue()
	t.ExpectEqual(lightning.Event{"specversion": "2.0", "contenttype": "text/plain", "data": []byte("hello")}, e)
}

func TestStructured(tt *testing.T) {
	t := test.New(tt)
	e := lightning.Event{"specversion": "2.0", "id": "foo", "data": "hello"}

	s, err := e.Format(lightning.JSONFormat)
	t.RequireNil(err)
	am, err := NewStructured(s)
	t.ExpectNil(err)
	t.ExpectEqual(lightning.JSONFormat.Name(), am.ContentType())
	t.ExpectEqual(0, len(am.ApplicationProperties()))
	var b []byte
	am.Unmarshal(&b)
	t.ExpectEqual(`{"data":"hello","id":"foo","specversion":"2.0"}`, string(b))

	m := Message{AMQP: am}
	s2 := m.Structured()
	t.Require(s2 != nil, "expect structured")
	e2, err := s2.Event()
	t.ExpectNil(err)
	t.ExpectEqual(e, e2)

	// Should also be able to extract directly as Event
	e3, err := m.Event()
	t.ExpectNil(err)
	t.ExpectEqual(e, e3)
}
