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

// package mqtt is an MQTT 3.1 implementation of the lightning cloud-event adapter
package mqtt

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alanconway/lightning/pkg/lightning"
	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

type SourceConfig struct {
	lightning.CommonConfig
	// Map topic filter to QoS (0,1 or 2)
	Filters map[string]byte
	// Capacity for Incoming() channel
	Capacity int
	// ClientID for MQTT connection
	ClientID string
	// AutoReconnect enables auto re-connect
	AutoReconnect bool
	// MaxReconnectInterval sets the max interval between re-connect attempts
	MaxReconnectInterval lightning.Duration
}

// NewSourceConfig sets non-zero defaults
func NewSourceConfig() *SourceConfig {
	h, _ := os.Hostname()
	clientid := fmt.Sprintf("%v-%v-%v", h, filepath.Base(os.Args[0]), os.Getpid())
	sc := &SourceConfig{
		Capacity:             1000,
		ClientID:             clientid,
		AutoReconnect:        true,
		MaxReconnectInterval: lightning.Duration{Duration: time.Second},
	}
	sc.URL.MustParse("tcp://:1883")
	return sc
}

type Source struct {
	// TODO: more configuration
	// - security, respect tls://, mqtts:// in URL, cert settings.
	// - QoS 3 - persistence options
	// - reconnect parameters - part of CommonConfig??
	// log output
	config    SourceConfig
	log       *zap.Logger
	client    paho.Client
	incoming  chan lightning.Message
	done      chan struct{}
	closeOnce sync.Once
	err       error
}

func NewSource(c *SourceConfig, l *zap.Logger) *Source {
	s := &Source{
		config:   *c,
		log:      l,
		incoming: make(chan lightning.Message, c.Capacity),
		done:     make(chan struct{}),
	}
	p, _ := c.URL.User.Password()
	opts := paho.NewClientOptions().AddBroker(c.URL.Host).SetClientID(c.ClientID)
	opts.SetUsername(c.URL.User.Username()).SetPassword(p)
	opts.SetConnectionLostHandler(s.onConnectionLost)
	opts.SetAutoReconnect(c.AutoReconnect)
	opts.SetMaxReconnectInterval(c.MaxReconnectInterval.Duration)
	s.client = paho.NewClient(opts)
	return s
}

func (s *Source) Incoming() <-chan lightning.Message { return s.incoming }

func (s *Source) Run() error {
	// paho will reconnect but won't retry initial connect, so do that here.
	s.err = s.retryConnect(s.config.AutoReconnect, s.config.MaxReconnectInterval.Duration, time.Second/10)
	if s.err != nil {
		return s.err
	}
	<-s.done
	return s.err
}

func (s *Source) Close() { s.closeErr(nil, true) }

func (s *Source) retryConnect(retry bool, max time.Duration, sleep time.Duration) error {
	for s.err = s.tryConnect(); lightning.IsRefused(s.err) && retry; s.err = s.tryConnect() {
		time.Sleep(sleep)
		sleep *= 2
		if sleep > max {
			sleep = max
		}
	}
	if s.err != nil {
		return s.err
	}
	if err := waitErr(s.client.SubscribeMultiple(s.config.Filters, s.onMessage)); err != nil {
		s.log.Error("cannot subscribe", zap.Error(err))
		s.closeErr(err, true)
	}
	return s.err
}

func waitErr(t paho.Token) error { t.Wait(); return t.Error() }

func (s *Source) tryConnect() error {
	s.log.Info("Connecting")
	err := waitErr(s.client.Connect())
	if err != nil {
		s.log.Error("Connect failed", zap.Error(err))
	} else {
		s.log.Debug("Connected")
	}
	return err
}

func (s *Source) closeErr(err error, disconnect bool) {
	s.closeOnce.Do(func() {
		if s.err == nil {
			s.err = err
		}
		if disconnect && s.client.IsConnected() {
			s.client.Disconnect(0)
		}
		close(s.done)
		close(s.incoming)
	})
}

func (s *Source) onMessage(c paho.Client, m paho.Message) {
	s.incoming <- &message{bytes.NewReader(m.Payload())}
}

func (s *Source) onConnectionLost(c paho.Client, err error) {
	s.log.Warn("Connection lost", zap.Error(err))
	s.closeErr(err, false)
}

type message struct{ reader io.Reader }

func (m *message) Event() (lightning.Event, error) { return m.Structured().Event() }
func (m *message) Structured() *lightning.Structured {
	return &lightning.Structured{Reader: m.reader, Format: lightning.JSONFormat}
}
