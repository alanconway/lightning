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
	"net"
	"net/http"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

// Binding is a factory to create Source/Sink with generic JSON configuration.
// Note Sink/Source can be used independently
type Binding struct{ log *zap.Logger }

func NewBinding() *Binding                      { return &Binding{log: zap.NewNop()} }
func (Binding) Name() string                    { return "http" }
func (b *Binding) SetLogger(logger *zap.Logger) { b.log = logger.Named("http") }

func (b *Binding) Source(c lightning.Config) (lightning.Source, error) {
	cc := lightning.CommonConfig{}
	cc.URL.MustParse("http://:80")
	if err := c.Unmarshal(&cc); err != nil {
		return nil, err
	}
	log := b.log.With(zap.String("source", cc.URL.String()))
	l, err := net.Listen("tcp", cc.URL.Host)
	if err != nil {
		return nil, err
	}
	s := NewServerSource(log)
	s.Start(l)
	return s, nil
}

func (b *Binding) Sink(c lightning.Config) (lightning.Sink, error) {
	cc := lightning.CommonConfig{}
	cc.URL.MustParse("http://:80")
	if err := c.Unmarshal(&cc); err != nil {
		return nil, err
	}
	s := &Sink{
		URL:    cc.URL.URL,
		Client: &http.Client{},
		Log:    b.log.With(zap.String("sink", cc.URL.String())),
	}
	return s, nil
}

func (Binding) Doc() string {
	return `
HTTP Source configuration:
{
 "binding": "http",
 "url": string          // Listening URL (source is server)
}

HTTP Sink configuration:
{
 "binding": "http",
 "url": string,         // Connect URL (sink is client)
}
`
}
