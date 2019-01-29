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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

type Sink struct {
	URL    *url.URL
	Client *http.Client
	Log    *zap.Logger
}

func (s *Sink) Close()      {}
func (s *Sink) Wait() error { return nil }

func (s *Sink) Send(m lightning.Message) error {
	req := http.Request{
		Method: http.MethodPost,
		URL:    s.URL,
		Header: http.Header{},
	}
	// Use existing structured message if there is one
	if s := m.Structured(); s != nil {
		req.Header.Set("content-type", s.Format.Name())
		req.Body = ioutil.NopCloser(s.Reader)
	} else {
		return (errors.New("binary HTTP events not implemented"))
	}
	// TODO aconway 2019-01-08: get more concurrency here?
	resp, err := s.Client.Do(&req)
	if err != nil {
		return (err)
	}
	defer resp.Body.Close()
	if resp.Status[0] != '2' {
		return (fmt.Errorf("Bad HTTP response: %s", resp.Status))
	}
	return nil
}
