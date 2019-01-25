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

import (
	"io"
	"io/ioutil"
)

// Message is a message created by a Source, containing a cloud-event
type Message interface {
	// Event decodes and returns the event.
	// Consumes event data, call at most once.
	Event() (Event, error)

	// Structured returns a structured (fully encoded) event if this
	// message contains one, nil if not.
	Structured() *Structured

	// Finish must be called by the Sink when it is done with the message.
	// TODO: Finish and QoS settings are not yet implemented
	Finish(error)
}

// Structured holds the entire event structure serialized as bytes.
type Structured struct {
	// Reader for encoded event bytes
	Reader io.Reader
	// Format used to encode/decode the event bytes
	Format Format
}

// Event decodes a structured event.
// Consumes event data, call at most once.
func (s *Structured) Event() (e Event, err error) {
	var b []byte
	if b, err = ioutil.ReadAll(s.Reader); err == nil {
		err = s.Format.Unmarshal(b, &e)
	}
	return
}
