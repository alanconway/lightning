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
	"io"
	"sync"

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// Source is an AMQP client that subscribes to AMQP addresses for events
type Source struct {
	config    SourceConfig
	log       *zap.Logger
	conn      electron.Connection
	incoming  chan lightning.Message
	done      chan struct{}
	closeOnce sync.Once
}

func NewSource(c *SourceConfig, log *zap.Logger) *Source {
	return &Source{
		config:   *c,
		log:      log,
		incoming: make(chan lightning.Message, c.Capacity),
		done:     make(chan struct{}),
	}
}

func (s *Source) Incoming() <-chan lightning.Message { return s.incoming }
func (s *Source) Close()                             { s.conn.Close(nil) }

func (s *Source) Run() (err error) {
	defer func() {
		if err != nil {
			s.log.Error("Run error", zap.Error(err))
		}
	}()
	if len(s.config.Addresses) == 0 {
		return errors.New("No Addresses for AMQP source")
	}

	if s.conn, err = electron.Dial("tcp", s.config.URL.Host); err != nil {
		return err
	}
	for _, a := range s.config.Addresses {
		go func(a string) {
			r, err := s.conn.Receiver(electron.Source(a), electron.Capacity(s.config.Capacity), electron.Prefetch(true))
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
	<-s.conn.Done()
	if s.conn.Error() == io.EOF {
		return nil
	} else {
		return s.conn.Error()
	}
}
