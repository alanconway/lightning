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

func NewSink(u *url.URL, c *http.Client, l *zap.Logger) *Sink {
	if c == nil {
		c = &http.Client{}
	}
	s := &Sink{URL: u, Client: c, Log: l.Named(lightning.UniqueID("http-sink"))}
	s.Log.Debug("client connecting", zap.String("URL", u.String()))
	return s
}
func (s *Sink) Close()      {}
func (s *Sink) Wait() error { return nil }

func (s *Sink) Send(m lightning.Message) (err error) {
	defer func() {
		if err != nil {
			s.Log.Error("send failed", zap.Error(err))
		}
	}()
	req := http.Request{
		Method: http.MethodPost,
		URL:    s.URL,
		Header: http.Header{},
	}
	if err := MakeMessage(m, &req); err != nil {
		return err
	}
	s.Log.Debug("sending", zap.Any("url", s.URL), zap.Any("headers", req.Header))
	resp, err := s.Client.Do(&req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.Status[0] != '2' {
		return (fmt.Errorf("Bad HTTP response: %s", resp.Status))
	}
	return nil
}
