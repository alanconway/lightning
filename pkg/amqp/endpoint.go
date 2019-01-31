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

	"github.com/alanconway/lightning/pkg/lightning"
	"go.uber.org/zap"
	"qpid.apache.org/electron"
)

// Endpoint implements lightning.Endpoint for AMQP sources and sinks
type Endpoint struct {
	log                    *zap.Logger
	connections, listeners sync.Map
	busy                   sync.WaitGroup
	err                    lightning.AtomicError
	closeOnce              sync.Once
	onClose                func()
}

func (e *Endpoint) init(log *zap.Logger, onClose func()) {
	e.log = log
	e.onClose = onClose
	e.busy.Add(1) // e.close() will call Done()
}

func (e *Endpoint) closeErr(err error) error {
	e.err.Set(err)
	e.Close()
	return e.err.Get()
}

func (e *Endpoint) Close() {
	e.closeOnce.Do(func() {
		e.listeners.Range(func(l, _ interface{}) bool {
			e.listeners.Delete(l)
			l.(net.Listener).Close()
			return true
		})
		e.connections.Range(func(c, _ interface{}) bool {
			e.connections.Delete(c)
			c.(electron.Connection).Close(nil)
			return true
		})
		e.onClose()
		e.err.Set(io.EOF)
		e.busy.Done()
	})
}

func (e *Endpoint) Wait() error {
	e.busy.Wait()
	return e.err.Get()
}

// Listeners returns the active listeners
func (e *Endpoint) Listeners() (l []net.Listener) {
	e.listeners.Range(func(v, _ interface{}) bool {
		l = append(l, v.(net.Listener))
		return true
	})
	return
}

// Listeners returns the active connections
func (e *Endpoint) Connections() (c []electron.Connection) {
	e.connections.Range(func(v, _ interface{}) bool {
		c = append(c, v.(electron.Connection))
		return true
	})
	return
}
