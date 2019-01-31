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
	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

// TODO aconway 2019-01-15: Use the same flexible client/server and
// link management configuration as https://github.com/alanconway/envoy-amqp
// For now this is a simple AMQP client that subscribes to N addresses

type SourceConfig struct {
	// URL connect to URL.Host, subscribe to AMQP address URL.Path
	URL lightning.URL
	// Capacity is the message credit window size for each link
	Capacity int
}

const BindingName = "amqp"

type Binding struct{ log *zap.Logger }

func NewBinding() *Binding                      { return &Binding{log: zap.NewNop()} }
func (Binding) Name() string                    { return BindingName }
func (b *Binding) SetLogger(logger *zap.Logger) { b.log = logger }

func (b *Binding) Source(c lightning.Config) (lightning.Source, error) {
	var sc SourceConfig
	if err := c.Unmarshal(&sc); err != nil {
		return nil, err
	}
	return NewClientSource(sc.URL.URL, sc.Capacity, b.log)
}

func (b Binding) Sink(c lightning.Config) (lightning.Sink, error) {
	var cc lightning.CommonConfig
	cc.URL.MustParse("amqp://:5672")
	if err := c.Unmarshal(&cc); err != nil {
		return nil, err
	}
	return NewClientSink(cc.URL.URL, b.log)
}

func (Binding) Doc() string {
	return `
AMQP Source configuration:
{
 "binding": "amqp"
 "url": string           // Connect URL for AMQP endpoint
 "addresses": [string]   // List of link addresses to subscribe to
 "capacity": int         // Credit window per link
}
`
}
