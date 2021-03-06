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
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alanconway/lightning/pkg/lightning"
	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

type SourceConfig struct {
	URL *url.URL
	// Map topic filter to QoS (0,1 or 2)
	Filters map[string]byte
	// Capacity to buffer incoming events
	Capacity int
	// ClientID for MQTT connection
	ClientID string
	// AutoReconnect enables auto re-connect
	AutoReconnect bool
	// MaxReconnectInterval sets the max interval between re-connect attempts
	MaxReconnectInterval time.Duration
}

// NewSourceConfig sets non-zero defaults
func NewSourceConfig() *SourceConfig {
	h, _ := os.Hostname()
	clientid := fmt.Sprintf("%v-%v-%v", h, filepath.Base(os.Args[0]), os.Getpid())
	sc := &SourceConfig{
		URL:                  &url.URL{Scheme: "tcp", Host: ":1883"},
		Capacity:             1000,
		ClientID:             clientid,
		AutoReconnect:        true,
		MaxReconnectInterval: time.Second,
		Filters:              make(map[string]byte),
	}
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

func NewSource(c *SourceConfig, l *zap.Logger) (*Source, error) {
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
	opts.SetMaxReconnectInterval(c.MaxReconnectInterval)
	s.client = paho.NewClient(opts)
	// paho will reconnect but won't retry initial connect, so do that here.
	err := s.retryConnect(c.AutoReconnect, c.MaxReconnectInterval, time.Second/10)
	return s, err
}

func (s *Source) Close()      { s.closeErr(nil, true) }
func (s *Source) Wait() error { <-s.done; return s.err }

func (s *Source) Receive() (lightning.Message, error) {
	if m, ok := <-s.incoming; ok {
		return m, nil
	} else {
		return nil, s.err
	}
}

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
	s.log.Debug("subscribing", zap.Any("filters", s.config.Filters))
	if err := waitErr(s.client.SubscribeMultiple(s.config.Filters, s.onMessage)); err != nil {
		s.log.Error("cannot subscribe", zap.Error(err))
		s.closeErr(err, true)
	}
	return s.err
}

func waitErr(t paho.Token) error { t.Wait(); return t.Error() }

func (s *Source) tryConnect() error {
	s.log.Debug("Connecting")
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
	s.log.Debug("received", zap.String("topic", m.Topic()))
	s.incoming <- message(m.Payload())
}

func (s *Source) onConnectionLost(c paho.Client, err error) {
	if err == io.EOF {
		s.log.Debug("Connection closed")
	} else {
		s.log.Warn("Connection lost", zap.Error(err))
	}
	s.closeErr(err, false)
}

type message []byte

func (m message) Event() (lightning.Event, error) { return m.Structured().Event() }
func (m message) Structured() *lightning.Structured {
	return &lightning.Structured{Bytes: ([]byte)(m), Format: lightning.JSONFormat}
}
