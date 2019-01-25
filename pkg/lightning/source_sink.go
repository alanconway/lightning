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

package lightning

// Source is a source of events.
//
// A Source implementation can be a client (connect to some
// server that provides events) or a server (accept connections
// from clients that provide events)
//
type Source interface {

	// Incoming channel of Message from the Source.
	//
	// Source blocks when the channel is full.
	// Channel closes when source is closed.
	Incoming() <-chan Message

	// Close tells the source to close and returns immediately.
	//
	// Incoming() is closed, Run() will return once Incoming() is
	// drained. Concurrent safe, can be called multiple times.
	Close()

	// Run the source, put messages on Incoming().
	//
	// Returns when the source closes.
	Run() error
}

// Sink is a destination for events.
//
// A Sink implementation can be a client (connect to some
// server that accepts events) or a server (accept connections
// from clients that subscribe for events)
//
type Sink interface {
	// Send a message to the sink, concurrent-safe.
	//
	// Message.Finish() will be called when the sink is finished with
	// the message. Depending on the implementation that may occur
	// during the call to Send() or later from another goroutine.
	Send(Message)

	// Close the sink, concurrent-safe.
	//
	// Further calls to Send() will return an error.  All outstanding
	// calls to Message.Finish() will occur before Close() returns.
	//
	// Can be called multiple times.
	Close()
}

// Transfer transfers events from source to sink until source is closed.
func Transfer(source Source, sink Sink) error {
	go func() {
		for m := range source.Incoming() {
			sink.Send(m)
		}
	}()
	return source.Run()
}
