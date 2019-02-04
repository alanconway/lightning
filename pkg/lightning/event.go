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
	"fmt"
)

// AttrNames provides attribute names that differ between cloudEvents 0.1 and 0.2
type AttrNames struct{ version, SpecVersion, ContentType, Data string }

var versions = []*AttrNames{
	&AttrNames{"0.1", "cloudEventsVersion", "contentType", data},
	&AttrNames{"0.2", "specversion", "contenttype", data},
}

// Data is the name of the data attribute.
const data = "data"

// Event is a simple name/value attribute map.
//
// Only the 'data', 'contenttype' and 'specversion' attributes are
// used in this package, others are simply name:value pairs.
// See Event.AttrNames() for dealing with attribute version differences
//
// TODO integrate with strictly-typed cloudevent APIs that validate
// specversion rules for attributes.
type Event map[string]interface{}

func (e Event) getStr(name string) string {
	if s, ok := e[name].(string); ok {
		return s
	} else {
		return ""
	}
}

// Names returns attribute names that vary by cloudEvents version.
// Uses the specversion of this event if set, else the latest known specversion.
func (e Event) Names() *AttrNames {
	var a *AttrNames
	for _, a = range versions {
		if v := e.getStr(a.SpecVersion); v == a.version {
			break
		}
	}
	return a // Default to latest
}

// SpecVersion or "" if absent.
func (e Event) SpecVersion() string { return e.getStr(e.Names().SpecVersion) }

// ContentType or "" if absent.
func (e Event) ContentType() string      { return e.getStr(e.Names().ContentType) }
func (e Event) SetContentType(ct string) { e[e.Names().ContentType] = ct }

func (e Event) Data() interface{}     { return e[data] }
func (e Event) SetData(v interface{}) { e[data] = v }

// FIXME aconway 2019-02-01: name of Format vs. Structured? Structured not a noun?
// FIXME aconway 2019-02-01: reader/bytes as a type, use for both
// event and structured...

func (e Event) Format(f Format) (*Structured, error) {
	if b, err := f.Marshal(e); err != nil {
		return nil, err
	} else {
		return &Structured{Bytes: b, Format: f}, nil
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

// DataString returns the data value as a string if it is of type
// string or []byte. Otherwise returns ("", false).
func (e Event) DataString() (string, bool) {
	switch d := e.Data().(type) {
	case string:
		return d, true
	case []byte:
		return string(d), true
	default:
		return "", false
	}
}

// DataBytes returns the data value as []byte if it is of type
// string or []byte. Otherwise returns (nil, false).
func (e Event) DataBytes() ([]byte, bool) {
	switch d := e.Data().(type) {
	case []byte:
		return d, true
	case string:
		return []byte(d), true
	default:
		return nil, false
	}
}
