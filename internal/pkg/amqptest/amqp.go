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

package amqptest

import (
	"errors"
	"log"
	"net"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type AMQPServer struct {
	l        net.Listener
	conn     electron.Connection
	Messages []amqp.Message // Send to each subscriber
	done     chan struct{}
	err      error
}

func NewAMQPServer(messages []amqp.Message) (*AMQPServer, error) {
	if l, err := net.Listen("tcp", ":0"); err == nil {
		return &AMQPServer{l: l, Messages: messages, done: make(chan struct{})}, nil
	} else {
		return nil, err
	}
}

func (s *AMQPServer) Done() <-chan struct{} { return s.done }
func (s *AMQPServer) Error() error          { return s.err }
func (s *AMQPServer) Host() string          { return s.l.Addr().String() }

func (s *AMQPServer) Run() (err error) {
	defer func() {
		log.Printf("FIXME AMQPServer Run Exit %v", err)
	}()
	cont := electron.NewContainer("btest-AMQPServer")
	if s.conn, s.err = cont.Accept(s.l); s.err != nil {
		log.Printf("FIXME Bad Acceptd %v", err)
		return s.err
	}
	s.l.Close() // This server only accepts one connection
	for in := range s.conn.Incoming() {
		switch in := in.(type) {
		case *electron.IncomingSender:
			go func(snd electron.Sender) {
				for _, m := range s.Messages {
					snd.SendForget(m)
				}
			}(in.Accept().(electron.Sender))
		case nil:
			return // Connection is closed
		default:
			in.Accept()
		}
	}
	<-s.conn.Done()
	s.err = s.conn.Error()
	close(s.done)
	return s.err
}

func (s *AMQPServer) Close() error {
	s.l.Close()
	if s.conn != nil {
		s.conn.Close(nil)
		return nil
	} else {
		return errors.New("AMQPServer is not running")
	}
}
