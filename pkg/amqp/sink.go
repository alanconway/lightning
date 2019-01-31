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
	"net"
	"net/url"
	"sync"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// Sink is an AMQP sink sends messages to one or more electron.Sender
type Sink struct {
	Endpoint
	senders sync.Map
}

func NewSink(log *zap.Logger) *Sink {
	var s Sink
	s.Endpoint.init(log, func() {})
	return &s
}

// Add a Sender to the sink. Event messages are sent to all senders.
func (s *Sink) Add(snd electron.Sender) {
	s.connections.Store(snd.Connection(), nil)
	s.log.Info("add", zap.String("sender", snd.String()))
	s.senders.Store(snd, nil)
}

func (s *Sink) Send(m lightning.Message) (err error) {
	s.log.Debug("send")
	am, err := NewMessage(m)
	if err == nil {
		s.senders.Range(func(k, _ interface{}) bool {
			// TODO aconway 2019-01-31: QoS 2, propagate accept/reject
			snd := k.(electron.Sender)
			snd.SendForget(am)
			if err = snd.Error(); err != nil {
				s.closeErr(err)
				return false
			}
			return true
		})
	}
	return
}

// Serve adds l to Listeners() and starts a server to Add() incoming Senders.
func (s *Sink) Serve(l net.Listener, opts ...electron.ConnectionOption) {
	s.log.Info("serve", zap.String("address", l.Addr().String()))
	s.listeners.Store(l, nil)
	s.busy.Add(1)
	opts = append(opts, electron.Server())
	go func() {
		defer s.busy.Done()
		for {
			c, err := accept(l, opts...)
			if err != nil {
				s.closeErr(err)
				return
			}
			go func() {
				for in := range c.Incoming() {
					switch in := in.(type) {
					case *electron.IncomingSender:
						snd := in.Accept().(electron.Sender)
						s.Add(snd)
					case nil:
						return // Connection is closed
					default:
						in.Accept()
					}
				}
			}()
		}
	}()
}

// NewClientSink creates a sink, connects to u.Host and sends to u.Path
func NewClientSink(u *url.URL, log *zap.Logger, opts ...electron.ConnectionOption) (*Sink, error) {
	if c, err := electron.Dial("tcp", u.Host, opts...); err != nil {
		return nil, err
	} else if snd, err := c.Sender(electron.Target(u.Path)); err != nil {
		c.Close(nil)
		return nil, err
	} else if err := snd.Sync(); err != nil {
		c.Close(nil)
		return nil, err
	} else {
		s := NewSink(log.Named("sink > " + u.String()))
		s.Add(snd)
		return s, nil
	}
}

// NewServerSink creates a server sink listening on network, address
func NewServerSink(network, address string, log *zap.Logger, opts ...electron.ConnectionOption) (*Sink, error) {
	if l, err := net.Listen(network, address); err != nil {
		return nil, err
	} else {
		s := NewSink(log.Named("sink < " + l.Addr().String()))
		s.Serve(l, opts...)
		return s, nil
	}
}
