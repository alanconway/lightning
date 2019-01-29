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
	"errors"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// Source is an AMQP client that subscribes to AMQP addresses for events
type Source struct {
	log      *zap.Logger
	conn     electron.Connection
	incoming chan lightning.Message
}

func (s *Source) Close()      { s.conn.Close(nil) }
func (s *Source) Wait() error { <-s.conn.Done(); return s.conn.Error() }

func (s *Source) Receive() (lightning.Message, error) {
	select {
	case <-s.conn.Done():
		return nil, s.conn.Error()
	case m := <-s.incoming:
		return m, nil
	}
}

func NewSource(c *SourceConfig, log *zap.Logger) (s *Source, err error) {
	s = &Source{
		log:      log,
		incoming: make(chan lightning.Message), // No capacity, AMQP Receivers do pre-fetching
	}
	if s.conn, err = electron.Dial("tcp", c.URL.Host); err != nil {
		return nil, err
	}
	if len(c.Addresses) == 0 {
		return nil, errors.New("No Addresses for AMQP source")
	}
	for _, a := range c.Addresses {
		go func(a string) {
			r, err := s.conn.Receiver(electron.Source(a), electron.Capacity(c.Capacity), electron.Prefetch(true))
			if err != nil {
				s.conn.Close(err)
				return
			}
			// TODO aconway 2019-01-15: timeouts etc
			for rm, err := r.Receive(); err == nil; rm, err = r.Receive() {
				s.log.Debug("Received", zap.String("message", rm.Message.String()))
				s.incoming <- Message{AMQP: rm.Message}
				// TODO aconway 2019-01-15: QoS 1, delay accept till hand-off to sink.
				rm.Accept()
			}
		}(a)
	}
	return
}
