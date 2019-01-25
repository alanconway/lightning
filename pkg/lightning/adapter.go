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

// Adapter connects a Source to a Sink.
type Adapter struct {
	Source Source
	Sink   Sink
}

// Run calls Source.Run() and transfers messages from Source to Sink
// until Source.Run() returns.
func (a *Adapter) Run() error {
	go func() {
		for m := range a.Source.Incoming() {
			a.Sink.Send(m)
		}
	}()
	return a.Source.Run()
}

// Close calls Source.Close() and Sink.Close()
func (a *Adapter) Close() {
	a.Source.Close()
	a.Sink.Close()
}

// ChanSink is a trivial sink that is just a channel.
//
// Allows an application to process events in memory.
type ChanSink chan Message

func MakeChanSink(cap int) ChanSink { return make(ChanSink, cap) }

// Send does not call m.Finish(), application must do so when finished.
func (s ChanSink) Send(m Message) { s <- m }
func (s ChanSink) Close()         { close(s) }
