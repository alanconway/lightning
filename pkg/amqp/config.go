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
	"net"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// TODO aconway 2019-01-15: Use the same flexible client/server and
// link management configuration as https://github.com/alanconway/envoy-amqp
// For now this is a simple AMQP client that subscribes to N addresses

type SourceConfig struct {
	// URL in CommonConfig is used for connection only, Addresses provides link addresses
	lightning.CommonConfig
	// Addresses are AMQP link addresses to subscribe to after connecting
	Addresses []string
	// Capacity is the message credit window size for each link
	Capacity int
}

func NewClientSource(c lightning.Config, log *zap.Logger) (lightning.Source, error) {
	var sc SourceConfig
	if err := c.Unmarshal(&sc); err != nil {
		return nil, err
	}
	conn, err := electron.Dial("tcp", sc.URL.Host)
	if err != nil {
		return nil, err
	}
	s := NewSource(log.With(zap.String("source", sc.URL.String())))
	if err = s.Connect(conn, sc.Addresses, sc.Capacity); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func NewServerSource(c lightning.Config, log *zap.Logger) (lightning.Source, error) {
	// TODO aconway 2019-01-31: ignores Addresses, need different config?
	var sc SourceConfig
	if err := c.Unmarshal(&sc); err != nil {
		return nil, err
	}
	s := NewSource(log.With(zap.String("source", sc.URL.String())))
	l, err := net.Listen("tcp", sc.URL.Host)
	if err != nil {
		s.Close()
		return nil, err
	}
	go s.Serve(l, electron.NewContainer(""), sc.Capacity)
	return s, nil
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
	conn, err := electron.Dial("tcp", sc.URL.Host)
	if err != nil {
		return nil, err
	}
	s := NewSource(b.log.With(zap.String("source", sc.URL.String())))
	if err = s.Connect(conn, sc.Addresses, sc.Capacity); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (b Binding) Sink(conf lightning.Config) (lightning.Sink, error) {
	return nil, fmt.Errorf("%v Sink not implemented", b.Name())
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
