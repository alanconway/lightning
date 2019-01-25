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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// FormatPrefix is the prefix for all structured cloud-event media types
var FormatPrefix = "application/cloudevents"

// IsFormat returns true if mediaType begins with application/cloudevents
func IsFormat(mediaType string) bool { return strings.HasPrefix(mediaType, FormatPrefix) }

// Format marshals and unmarshal structured events.
type Format interface {
	// The media-type name of the format, e.g. "application/cloudevents+json"
	Name() string
	// Marshal event, return bytes
	Marshal(Event) ([]byte, error)
	// Unmarshal bytes into Event
	Unmarshal([]byte, *Event) error
}

// UnknownFormat is a Format and an error.
// Name() returns the unknown name, Marshal() and Unmarshal() return error.
type UnknownFormat string

func (u UnknownFormat) Name() string { return string(u) }
func (u UnknownFormat) Error() string {
	return fmt.Sprintf("Unknown cloud event format %#v", u)
}
func (u UnknownFormat) Marshal(e Event) ([]byte, error)    { return nil, u }
func (u UnknownFormat) Unmarshal(b []byte, e *Event) error { return u }

// JSONFormat is the standard "application/cloudEvents+json" format
var JSONFormat = jsonFormat{}

type jsonFormat struct{}

func (jsonFormat) Name() string { return "application/cloudevents+json" }

func (jsonFormat) Marshal(e Event) ([]byte, error) {
	ct := e.ContentType()
	if MediaTypeIsBinary(ct) {
		// Binary media, needs base64 encoding
		d, err := e.DataValue()
		if err != nil {
			return nil, err
		}
		b, ok := d.([]byte)
		if !ok {
			return nil, fmt.Errorf("Expected binary data value in event %#v", e)
		}
		e[data] = base64.StdEncoding.EncodeToString(b)
	}
	return json.Marshal(e)
}

// FIXME aconway 2019-01-16: sanitize Event to convert to legal values
// e.g. json numbers unmarshal as float64, need to be intergerized.

func (jsonFormat) Unmarshal(b []byte, e *Event) error {
	return json.Unmarshal(b, e)
}

// FormatMap is a map of formats by name
type FormatMap map[string]Format

// Get returns a Format for name.
// If name is not in the map it returns UnknownFormat(name)
func (m FormatMap) Get(name string) Format {
	if f := m[name]; f != nil {
		return f
	} else {
		return UnknownFormat(name)
	}
}

// Formats is a map of known formats by name.
// Initially includes JSONFormat, you can add others.
var Formats = FormatMap{JSONFormat.Name(): JSONFormat}
