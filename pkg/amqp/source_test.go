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
	"io/ioutil"
	"net/url"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/amqptest"
	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/amqp"
)

func TestSource(tt *testing.T) {
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

	sink := lightning.MakeChanSink(100)
	logger, _ := zap.NewDevelopment()

	sc := SourceConfig{Addresses: []string{"foo"}, Capacity: 100}
	sc.URL.URL = &url.URL{Host: srv.Host()}
	src := NewSource(&sc, logger)

	go (&lightning.Adapter{Source: src, Sink: sink}).Run()

	// First message should be binary
	m := <-sink
	s := m.Structured()
	t.ExpectEqual((*lightning.Structured)(nil), s)
	e, err := m.Event()
	t.ExpectNil(err)
	b, err := ioutil.ReadAll(e.DataReader())
	t.ExpectNil(err)
	t.ExpectEqual("binary", string(b))

	// Next message should be structured
	m = <-sink
	s = m.Structured()
	t.ExpectEqual(lightning.JSONFormat.Name(), s.Format.Name())
	e, err = s.Event()
	t.ExpectNil(err)
	t.ExpectEqual("structured", e["data"])
}
