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

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
)

const BindingName = "amqp"

type Binding struct{ log *zap.Logger }

func NewBinding() *Binding                      { return &Binding{log: zap.NewNop()} }
func (Binding) Name() string                    { return BindingName }
func (b *Binding) SetLogger(logger *zap.Logger) { b.log = logger }

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

func (b *Binding) Source(c lightning.Config) (lightning.Source, error) {
	var sc SourceConfig
	if err := c.Unmarshal(sc); err != nil {
		return nil, err
	}
	return NewSource(&sc, b.log.With(zap.String("source", sc.URL.String()))), nil
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
