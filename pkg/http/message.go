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
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/alanconway/lightning/pkg/lightning"
)

var cePrefix = "ce-"

func MakeStructured(s *lightning.Structured, req *http.Request) {
	req.Header.Set("content-type", s.Format.Name())
	req.Body = ioutil.NopCloser(s.Reader)
}

func headerString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v) // TODO aconway 2019-01-16: slow and sloppy
	}
}

func MakeBinary(e lightning.Event, req *http.Request) error {
	ct := e.ContentType()
	if ct == "" || e.DataReader() == nil {
		// Can't make binary event, fall back to structured JSON
		s, err := e.Format(lightning.JSONFormat)
		if err != nil {
			return err
		}
		MakeStructured(s, req)
	} else {
		req.Header.Set("content-type", ct)
		for k, v := range e {
			a := e.Attributes()
			// Data and content type are on HTTP headers and body
			if k != a.Data && k != a.ContentType {
				req.Header.Set(cePrefix+k, headerString(v))
			}
		}
	}
	return nil
}

func MakeMessage(m lightning.Message, req *http.Request) error {
	if s := m.Structured(); s != nil {
		MakeStructured(s, req)
		return nil
	}
	if e, err := m.Event(); err == nil {
		return MakeBinary(e, req)
	} else {
		return err
	}
}

type Message struct{ Req *http.Request }

func (m Message) Structured() *lightning.Structured {
	ct := m.Req.Header.Get("content-type")
	if lightning.IsFormat(ct) {
		return &lightning.Structured{Reader: m.Req.Body, Format: lightning.Formats.Get(ct)}
	}
	return nil
}

func (m Message) Event() (lightning.Event, error) {
	if s := m.Structured(); s != nil {
		return s.Event()
	}
	e := make(lightning.Event)
	for k, v := range m.Req.Header {
		n := strings.ToLower(k)
		if strings.HasPrefix(n, cePrefix) {
			e[strings.TrimPrefix(n, cePrefix)] = v
		}
	}
	attrs := e.Attributes()
	ct := m.Req.Header.Get("Content-Type")
	if ct != "" {
		e[attrs.ContentType] = ct
	}
	e.SetData(m.Req.Body)
	return e, nil
}
