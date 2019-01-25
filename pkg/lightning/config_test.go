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
	"net/url"
	"testing"
	"time"

	"github.com/alanconway/lightning/internal/pkg/test"
)

func TestURL(tt *testing.T) {
	t := test.New(tt)
	var u URL
	err := json.Unmarshal([]byte(`"http://foo/bar"`), &u)
	t.ExpectNil(err)
	t.ExpectEqual(&url.URL{Scheme: "http", Host: "foo", Path: "/bar"}, u.URL)
}

func TestDuration(tt *testing.T) {
	t := test.New(tt)
	var d Duration
	err := json.Unmarshal([]byte(`"1h5m"`), &d)
	t.ExpectNil(err)
	t.ExpectEqual(time.Hour+5*time.Minute, d.Duration)
	err = json.Unmarshal([]byte(`"x"`), &d)
	t.Expect(err != nil, "expect error")
}

func TestCommonConfig(tt *testing.T) {
	t := test.New(tt)
	var cc CommonConfig
	err := json.Unmarshal([]byte(`{"binding":"blob", "url":"http://foo/bar"}`), &cc)
	t.ExpectNil(err)
	t.ExpectEqual(&url.URL{Scheme: "http", Host: "foo", Path: "/bar"}, cc.URL.URL)
	t.ExpectEqual("blob", cc.Binding)
}
