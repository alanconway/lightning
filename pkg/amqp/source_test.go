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
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/amqptest"
	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	"qpid.apache.org/amqp"
)

func TestClientSource(tt *testing.T) {
	t := test.New(tt)

	bmsg := amqp.NewMessageWith([]byte("binary"))
	bmsg.SetContentType("text/plain")
	bmsg.SetApplicationProperties(map[string]interface{}{"cloudevents:specversion": "2.0"})

	smsg := amqp.NewMessageWith([]byte(`{"specversion": "2.0", "data": "structured"}`))
	smsg.SetContentType(lightning.JSONFormat.Name())

	amqpMsgs := []amqp.Message{bmsg, smsg}

	srv, err := amqptest.NewAMQPServer(amqpMsgs)
	t.RequireNil(err)
	go srv.Run()
	defer func() { t.ExpectNil(srv.Close()) }()

	sc := fmt.Sprintf(`{"addresses":["foo"], "capacity": 100, "URL": "//%s"}`, srv.Host())
	src, err := NewBinding().Source(lightning.Config(sc))
	t.ExpectNil(err)
	defer src.Close()

	// First message should be binary
	m, err := src.Receive()
	t.ExpectNil(err)
	s := m.Structured()
	t.ExpectEqual((*lightning.Structured)(nil), s)
	e, err := m.Event()
	t.ExpectNil(err)
	b, err := ioutil.ReadAll(e.DataReader())
	t.ExpectNil(err)
	t.ExpectEqual("binary", string(b))

	// Next message should be structured
	m, err = src.Receive()
	t.ExpectNil(err)
	s = m.Structured()
	t.ExpectEqual(lightning.JSONFormat.Name(), s.Format.Name())
	e, err = s.Event()
	t.ExpectNil(err)
	t.ExpectEqual("structured", e["data"])
}
