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
	"bytes"
	"io"
	"io/ioutil"
	"strings"

	"github.com/alanconway/lightning/pkg/lightning"
	"qpid.apache.org/amqp"
)

type Message struct{ AMQP amqp.Message }

func NewStructured(s *lightning.Structured) (amqp.Message, error) {
	d, err := ioutil.ReadAll(s.Reader)
	if err == nil {
		am := amqp.NewMessageWith(d)
		am.SetContentType(s.Format.Name())
		return am, nil
	}
	return nil, err
}

func NewBinary(e lightning.Event) (am amqp.Message, err error) {
	attrs := e.Attributes()
	var d interface{}
	if d, err = e.DataValue(); err == nil {
		am = amqp.NewMessageWith(d)
		if ct, ok := e[attrs.ContentType].(string); ok {
			am.SetContentType(ct)
		}
		props := make(map[string]interface{})
		for k, v := range e {
			// Data and content type are set on AMQP body and content-type
			if k != attrs.Data && k != attrs.ContentType {
				props[cePrefix+k] = v
			}
			am.SetApplicationProperties(props)
		}
	}
	return
}

func NewMessage(m lightning.Message) (am amqp.Message, err error) {
	if s := m.Structured(); s != nil {
		return NewStructured(s)
	} else {
		e, err := m.Event()
		if err != nil {
			return nil, err
		}
		return NewBinary(e)
	}
}

// BodyReader for binary AMQP body - error if body is not binary
func (m Message) BodyReader() (r io.Reader, err error) {
	// TODO aconway 2019-01-16: bug in amqp.Message.Unmarshal - leaks panic
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
	var b []byte
	m.AMQP.Unmarshal(&b)
	return bytes.NewReader(b), nil
}

func (m Message) Structured() *lightning.Structured {
	// Must have binary body and a cloud-event format ContentType
	ct := m.AMQP.ContentType()
	if lightning.IsFormat(ct) {
		r, err := m.BodyReader()
		if err == nil {
			return &lightning.Structured{Reader: r, Format: lightning.Formats.Get(ct)}
		}
	}
	return nil
}

var cePrefix = "cloudevents:"

func (m Message) Event() (lightning.Event, error) {
	if s := m.Structured(); s != nil {
		return s.Event()
	}
	e := make(lightning.Event)
	for k, v := range m.AMQP.ApplicationProperties() {
		if strings.HasPrefix(k, cePrefix) {
			e[strings.TrimPrefix(k, cePrefix)] = v
		}
	}
	attrs := e.Attributes()
	if e[attrs.ContentType] == nil && m.AMQP.ContentType() != "" {
		e[attrs.ContentType] = m.AMQP.ContentType()
	}
	if e[attrs.ContentType] == nil { // Native data type
		switch b := m.AMQP.Body().(type) {
		case amqp.Binary:
			e.SetData([]byte(b))
		default:
			// TODO aconway 2019-01-16: check this is a legal cloud event type
			e.SetData(b)
		}
	} else { // content-type described bytes
		r, err := m.BodyReader()
		if err != nil {
			return nil, err
		}
		e.SetData(r)
	}
	return e, nil
}
