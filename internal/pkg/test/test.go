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

package test

import (
	"fmt"
	"reflect"
	"regexp"
	"testing"
)

func Nil(v interface{}) (bool, string) { return v == nil, fmt.Sprintf("nil != %v", v) }

func Equal(want, got interface{}) (bool, string) {
	if reflect.TypeOf(want) != reflect.TypeOf(got) {
		return false, fmt.Sprintf("type mismatch: (%T)%#v != (%T)%#v)", want, want, got, got)
	} else {
		if reflect.DeepEqual(want, got) {
			return true, ""
		} else {
			return false, fmt.Sprintf("%#v != %#v)", want, got)
		}
	}
}

func Match(re string, v interface{}) (bool, string) {
	var b []byte
	switch v := v.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	case error:
		b = []byte(v.Error())
	default:
		return false, fmt.Sprintf("cannot match /%s/ in (%T)%#v", re, v, v)
	}
	m, err := regexp.Match(re, b)
	if err != nil {
		return false, fmt.Sprintf("regex error for /%v/ in %#v: %v)", re, v, err)
	} else {
		return m, fmt.Sprintf("no match for /%v/ in %#v)", re, v)
	}
}

type TB struct{ testing.TB }

func New(t testing.TB) TB { return TB{t} }

func (tb TB) Expect(b bool, msg string) {
	tb.Helper()
	if !b {
		tb.Error(msg)
	}
}

func (tb TB) Expectf(b bool, format string, arg ...interface{}) {
	tb.Helper()
	tb.Expect(b, fmt.Sprintf(format, arg...))
}

func (tb TB) Require(b bool, msg string) {
	tb.Helper()
	if !b {
		tb.Fatal(msg)
	}
}

func (tb TB) Requiref(b bool, format string, arg ...interface{}) {
	tb.Helper()
	tb.Require(b, fmt.Sprintf(format, arg...))
}

func (t TB) ExpectNil(v interface{})              { t.Helper(); t.Expect(Nil(v)) }
func (t TB) ExpectEqual(want, got interface{})    { t.Helper(); t.Expect(Equal(want, got)) }
func (t TB) ExpectMatch(re string, s interface{}) { t.Helper(); t.Expect(Match(re, s)) }

func (t TB) RequireNil(v interface{})              { t.Helper(); t.Require(Nil(v)) }
func (t TB) RequireEqual(want, got interface{})    { t.Helper(); t.Require(Equal(want, got)) }
func (t TB) RequireMatch(re string, s interface{}) { t.Helper(); t.Require(Match(re, s)) }
