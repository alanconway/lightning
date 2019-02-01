/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, SpecVersion 2.0 (the
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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

// Attributes provides attribute names that differ between cloudEvents 0.1 and 0.2
type Attributes struct{ version, SpecVersion, ContentType, Data string }

var versions = []*Attributes{
	&Attributes{"0.1", "cloudEventsVersion", "contentType", data},
	&Attributes{"0.2", "specversion", "contenttype", data},
}

const data = "data"

// Event is a simple name/value attribute map.
//
// Only the 'data', 'contenttype' and 'specversion' attributes are
// used in this package, others are simply name:value pairs.
// See Event.Attributes() for dealing with attribute version differences
//
// the 'data' attribute may be an io.Reader or []byte for binary data,
// or a Go native value.
//
type Event map[string]interface{}

func (e Event) getStr(name string) string {
	if s, ok := e[name].(string); ok {
		return s
	} else {
		return ""
	}
}

// Attributes returns attribute names that vary by cloudEvents, appropriate for this event.
// Returns nil if the event has no known specversion attribute.
func (e Event) Attributes() *Attributes {
	var a *Attributes
	for _, a = range versions {
		if v := e.getStr(a.SpecVersion); v == a.version {
			break
		}
	}
	return a // Default to latest
}

// ContentType returns the contenttype
func (e Event) ContentType() string { return e.getStr(e.Attributes().ContentType) }

// SpecVersion returns the specversion
func (e Event) SpecVersion() string { return e.getStr(e.Attributes().SpecVersion) }

// DataIsBytes returns true if the data attribute types is []bytes or io.Reader
func (e Event) DataIsBytes() bool {
	switch e[data].(type) {
	case []byte:
		return true
	case io.Reader:
		return true
	default:
		return false
	}
}

// DataReader returns a reader if the data attribute types is []bytes or io.Reader
// nil otherwise.
func (e Event) DataReader() io.Reader {
	switch d := e[data].(type) {
	case []byte:
		return bytes.NewReader(d) // Wrap []byte data in a reader
	case io.Reader:
		return d // Data is already a reader
	default:
		return nil // Data is not binary
	}
}

// DataValue returns the data attribute as a Go value.
// Note if data is an io.Reader it is read immediately and replaced with []byte.
func (e Event) DataValue() (interface{}, error) {
	var err error
	if r, ok := e[data].(io.Reader); ok {
		e[data], err = ioutil.ReadAll(r) // Read entire value
	}
	return e[data], err
}

func (e Event) SetData(v interface{}) {
	e[data] = v
}

// FIXME aconway 2019-02-01: name of Format vs. Structured? Structured not a noun?
// FIXME aconway 2019-02-01: reader/bytes as a type, use for both
// event and structured...

func (e Event) Format(f Format) (*Structured, error) {
	if b, err := f.Marshal(e); err == nil {
		return &Structured{bytes.NewReader(b), f}, nil
	} else {
		return &Structured{}, err
	}
}

// Event returns the event itself - required by Message interface.
func (e Event) Event() (Event, error) { return e, nil }

// Structured returns nil - required by Message interface.
func (e Event) Structured() *Structured { return nil }

func (e Event) String() string { return fmt.Sprintf("%#v", e) }

func (e Event) Copy() Event {
	e2 := make(Event, len(e))
	for k, v := range e {
		e2[k] = v
	}
	return e2
}
