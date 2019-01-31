/*
Lliicensed to the Apache Software Foundation (ASF) under one
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

package lightning

import "fmt"

// Endpoint is implemented by sources and sinks
type Endpoint interface {
	// Close initiates shut-down of the endpoint, concurrent safe.
	// Further use will return an error, io.EOF if there is no other error.
	Close()

	// Wait for shut-down to complete, concurrent safe.
	// Returns the error that caused the shut-down, or io.EOF if Close() was called.
	Wait() error
}

// Source is a source of events.
type Source interface {
	Endpoint

	// Receive the next event Message from the Source. Concurrent safe.
	Receive() (Message, error)
}

// Sink is a destination for events.
type Sink interface {
	Endpoint

	// Send a message in the sink, concurrent-safe.
	Send(Message) error
}

// Transfer events from source to sink until one of them returns an error.
func Transfer(source Source, sink Sink) (err error) {
	var m Message
	for m, err = source.Receive(); err == nil; m, err = source.Receive() {
		if m == nil {
			panic(fmt.Sprintf("got nil from %T", source))
		}
		err = sink.Send(m)
	}
	return
}
