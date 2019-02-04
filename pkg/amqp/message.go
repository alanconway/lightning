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
	"strings"

	"github.com/alanconway/lightning/pkg/lightning"
	"qpid.apache.org/amqp"
)

type Message struct{ AMQP amqp.Message }

func NewStructured(s *lightning.Structured) amqp.Message {
	am := amqp.NewMessageWith(s.Bytes)
	am.SetContentType(s.Format.Name())
	return am
}

func NewBinary(e lightning.Event) amqp.Message {
	am := amqp.NewMessageWith(e.Data())
	if ct := e.ContentType(); ct != "" {
		am.SetContentType(ct)
	}
	props := make(map[string]interface{})
	attrs := e.Names()
	for k, v := range e {
		// Data and content type are set on AMQP body and content-type
		if k != attrs.Data && k != attrs.ContentType {
			props[AttributePrefix+k] = v
		}
		am.SetApplicationProperties(props)
	}
	return am
}

func NewMessage(m lightning.Message) (am amqp.Message, err error) {
	if s := m.Structured(); s != nil {
		return NewStructured(s), nil
	} else {
		e, err := m.Event()
		if err != nil {
			return nil, err
		}
		return NewBinary(e), nil
	}
}

func Unmarshal(m amqp.Message, v interface{}) (err error) {
	// TODO aconway 2019-01-16: workaround bug in amqp.Message.Unmarshal - panics
	defer func() {
		r := recover()
		switch r := r.(type) {
		case nil:
		case (*amqp.UnmarshalError):
			err = r
		default:
			panic(r)
		}
	}()
	m.Unmarshal(v)
	return nil
}

func (m Message) Structured() *lightning.Structured {
	var b []byte
	if ct := m.AMQP.ContentType(); !lightning.IsFormat(ct) {
		return nil
	} else if err := Unmarshal(m.AMQP, &b); err != nil {
		return nil
	} else {
		return &lightning.Structured{Bytes: b, Format: lightning.Formats.Get(ct)}
	}
}

// AttributePrefix is pre-pended to cloud-event attribute names when
// then are stored as AMQP application-properties
var AttributePrefix = "cloudEvents:"

func (m Message) Event() (lightning.Event, error) {
	if s := m.Structured(); s != nil {
		return s.Event()
	}
	e := make(lightning.Event)
	for k, v := range m.AMQP.ApplicationProperties() {
		if strings.HasPrefix(k, AttributePrefix) {
			e[strings.TrimPrefix(k, AttributePrefix)] = v
		}
	}
	if ct := m.AMQP.ContentType(); ct != "" {
		e.SetContentType(ct)
	}
	switch b := m.AMQP.Body().(type) {
	case amqp.Binary: // Funky Binary type used by amqp package
		e.SetData([]byte(b))
	default:
		e.SetData(b)
	}
	return e, nil
}
