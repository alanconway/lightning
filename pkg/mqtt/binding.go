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

package mqtt

import (
	"fmt"
	"log"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Binding struct{ log *zap.Logger }

func NewBinding() *Binding     { return &Binding{log: zap.NewNop()} }
func (b Binding) Name() string { return "mqtt" }

func pahoLog(logger *zap.Logger, level zapcore.Level) *log.Logger {
	// Ignore errors - we won't pass an invalid level
	l, _ := zap.NewStdLogAt(logger, level)
	return l
}

func (b *Binding) SetLogger(logger *zap.Logger) {
	b.log = logger
	// TODO aconway 2019-01-15: noisy, but maybe useful for debugging.
	// Introduce better logging controls to enable?
	// l := logger.Named("paho")
	// paho.DEBUG = pahoLog(l, zap.DebugLevel)
	// paho.WARN = pahoLog(l, zap.WarnLevel)
	// paho.ERROR = pahoLog(l, zap.ErrorLevel)
	// paho.CRITICAL = pahoLog(l, zap.FatalLevel)
}

func (b *Binding) Source(c lightning.Config) (lightning.Source, error) {
	sc := NewSourceConfig()
	if err := c.Unmarshal(sc); err != nil {
		return nil, err
	}
	return NewSource(sc, b.log.With(zap.String("source", sc.URL.String()))), nil
}

func (b Binding) Sink(conf lightning.Config) (lightning.Sink, error) {
	return nil, fmt.Errorf("%v Sink not implemented", b.Name())
}

func (Binding) Doc() string {
	// TODO aconway 2019-01-15: generate config doc from struct - reflect or godoc?
	return `
MQTT Source configuration:
{
 "binding": "mqtt"
 "url": string              // Connect URL for MQTT broker
 "filters":  {string: int}  // Map of topic filter key, QoS value (0, 1 or 2)
 "capacity": int            // Max messages to buffer
 "clientid": string         // MQTT client-id string
}
`
}
