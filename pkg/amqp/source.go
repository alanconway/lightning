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
under the License. *
*/

package amqp

import (
	"io"
	"net"
	"net/url"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// Source is an AMQP source that receives messages from a collection of electron.Receiver
type Source struct {
	Endpoint
	incoming chan lightning.Message
}

// NewSource returns a new Source, use Add to add receivers.
func NewSource(log *zap.Logger) *Source {
	var s Source
	s.Endpoint.init(log, func() { close(s.incoming) })
	s.incoming = make(chan lightning.Message)
	return &s
}

// Receive the next event message from the source.
func (s *Source) Receive() (lightning.Message, error) {
	if m, ok := <-s.incoming; ok {
		return m, nil
	} else {
		return nil, s.err.Get()
	}
}

// Add a Receiver to the source and start receiving messages from it
func (s *Source) Add(r electron.Receiver) {
	s.log.Info("add", zap.String("receiver", r.String()))
	s.connections.Store(r.Connection(), nil)
	s.busy.Add(1)
	go func() {
		defer s.busy.Done()
		rm, err := r.Receive()
		for ; err == nil; rm, err = r.Receive() {
			s.log.Debug("received", zap.Any("amqp message", rm.Message))
			s.incoming <- Message{AMQP: rm.Message}
			rm.Accept() // TODO aconway 2019-01-15: QoS 1, delay accept till hand-off to sink.
		}
		if err != io.EOF {
			s.closeErr(err)
		}
	}()
}

func logErr(log *zap.Logger, msg string, err error, fields ...zap.Field) {
	if err != nil {
		fields := append(fields, zap.Error(err))
		log.Error(msg, fields...)
	}
}

func accept(listener net.Listener, opts ...electron.ConnectionOption) (c electron.Connection, err error) {
	conn, err := listener.Accept()
	if err == nil {
		if c, err = electron.NewConnection(conn, opts...); err != nil {
			conn.Close()
		}
	}
	return
}

// FIXME aconway 2019-02-01: specify allowed sources

// Serve adds l to Listeners() and starts a server to Add() incoming Receivers.
func (s *Source) Serve(l net.Listener, capacity int, opts ...electron.ConnectionOption) {
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
					case *electron.IncomingReceiver:
						in.SetCapacity(capacity)
						in.SetPrefetch(true)
						s.Add(in.Accept().(electron.Receiver))
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

// NewClientSource creates a source connected to u.Host and subscribed to u.Path
func NewClientSource(u *url.URL, capacity int, logger *zap.Logger, opts ...electron.ConnectionOption) (*Source, error) {
	if c, err := electron.Dial("tcp", u.Host, opts...); err != nil {
		return nil, err
	} else if r, err := c.Receiver(electron.Source(u.Path), electron.Capacity(capacity), electron.Prefetch(true)); err != nil {
		c.Close(nil)
		return nil, err
	} else if err := r.Sync(); err != nil {
		c.Close(nil)
		return nil, err
	} else {
		s := NewSource(logger.Named(lightning.UniqueID("amqp-source")))
		s.log.Info("connected", zap.String("url", u.String()))
		s.Add(r)
		return s, nil
	}
}

// NewServerSource creates a source listening on network, address.
// Only links with sources in the allowed list are accepted, unless
// allowed is empty, in which case all links are accepted.
func NewServerSource(network, address string, allowed []string, capacity int, log *zap.Logger, opts ...electron.ConnectionOption) (*Source, error) {
	if l, err := net.Listen(network, address); err != nil {
		return nil, err
	} else {
		s := NewSource(log.Named(lightning.UniqueID("amqp-source")))
		s.log.Info("serving", zap.String("addr", l.Addr().String()))
		s.Serve(l, capacity, opts...)
		return s, nil
	}
}
