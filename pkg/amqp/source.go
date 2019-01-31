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
	"sync"
	"sync/atomic"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// Source is an AMQP source that receives messages from a collection of electron.Receiver
type Source struct {
	log                    *zap.Logger
	connections, listeners sync.Map
	incoming               chan lightning.Message
	workers                sync.WaitGroup
	err                    atomic.Value
	waitOnce               sync.Once
}

// NewSource returns a new Source, use Add to add receivers.
func NewSource(log *zap.Logger) *Source {
	return &Source{log: log, incoming: make(chan lightning.Message)}
}

func (s *Source) setErr(err error) {
	if s.err.Load() == nil {
		s.err.Store(err)
	}
}

// Add a Receiver to the source and start receiving messages from it
func (s *Source) Add(r electron.Receiver) {
	s.connections.Store(r.Connection(), nil)
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		rm, err := r.Receive()
		for ; err == nil; rm, err = r.Receive() {
			s.incoming <- Message{AMQP: rm.Message}
			rm.Accept() // TODO aconway 2019-01-15: QoS 1, delay accept till hand-off to sink.
		}
		if err != io.EOF {
			s.setErr(err)
			s.Close()
		}
	}()
}

func (s *Source) Close() {
	s.listeners.Range(func(l, _ interface{}) bool {
		s.connections.Delete(l)
		l.(net.Listener).Close()
		return true
	})
	s.connections.Range(func(c, _ interface{}) bool {
		s.connections.Delete(c)
		c.(electron.Connection).Close(nil)
		return true
	})
}

func (s *Source) Wait() error {
	s.waitOnce.Do(func() {
		s.workers.Wait()
		s.setErr(io.EOF)
		close(s.incoming)
	})
	return s.err.Load().(error)
}

func (s *Source) Receive() (lightning.Message, error) {
	if m, ok := <-s.incoming; ok {
		return m, nil
	} else {
		return nil, s.err.Load().(error)
	}
}

// Connect adds Receivers from conn for each source address in addrs using opts
func (s *Source) Connect(conn electron.Connection, addrs []string, capacity int) error {
	for _, a := range addrs {
		r, err := conn.Receiver(electron.Source(a), electron.Capacity(capacity), electron.Prefetch(true))
		if err != nil {
			s.Close()
			return err
		}
		s.Add(r)
	}
	return nil
}

func (s *Source) Serve(l net.Listener, container electron.Container, capacity int) {
	s.listeners.Store(l, nil)
	for {
		conn, err := container.Accept(l)
		if err != nil {
			s.setErr(err)
			s.Close()
			return
		}
		go func() {
			for in := range conn.Incoming() {
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
}
