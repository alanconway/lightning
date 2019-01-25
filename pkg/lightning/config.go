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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"
)

// URL wrapper that can unmarshal from JSON or any TextUnmarshaler-aware encoding.
type URL struct{ *url.URL }

func (u *URL) Parse(s string) (err error)   { u.URL, err = url.Parse(s); return }
func (u *URL) UnmarshalText(b []byte) error { return u.Parse(string(b)) }
func (u *URL) MustParse(s string) {
	if err := u.Parse(s); err != nil {
		panic(err)
	}
}

// Duration that can unmarshal from JSON ro any TextUnmarshaler-aware encoding
//
// Uses time.ParseDuration() format e.g. as "300ms", "-1.5h" or "2h45m".
// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalText(b []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(b))
	return
}

// Common configuration for all sources and sinks
// Each source/sink implementation will have additional information
type CommonConfig struct {
	// Binding name - required for all source/sink configurations
	Binding string
	// URL to connect or listen
	URL URL
	// TODO: common TLS certificate configuration?
}

// Config is JSON configuration bytes for a source or sink
type Config []byte

func (c Config) String() string { return string([]byte(c)) }
func (c Config) Bytes() []byte  { return []byte(c) }

// MakeConfig makes a Configuration.
//
// The string s the name of an existing file to read, a URL to GET, or
// a direct JSON object string.
//
func MakeConfig(s string) (c Config, err error) {
	var b []byte
	if f, err2 := os.Open(s); !os.IsNotExist(err2) { // File exists
		if err = err2; err == nil {
			b, err = ioutil.ReadAll(f)
		}
	} else if u, _ := url.Parse(s); u != nil && u.Scheme != "" { // URL with scheme
		res, err := http.Get(s)
		if err == nil {
			if res.Status[0] == '2' {
				b, err = ioutil.ReadAll(res.Body)
			} else {
				err = fmt.Errorf("bad response: %v", res)
			}
		}
	} else {
		b = []byte(s)
	}
	return Config(b), err
}

// Unmarshal configuration bytes into a configruation struct v
func (c Config) Unmarshal(v interface{}) error { return json.Unmarshal([]byte(c), v) }
