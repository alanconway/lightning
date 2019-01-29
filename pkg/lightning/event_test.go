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
	"io"
	"io/ioutil"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/test"
)

func TestEventAttrs(tt *testing.T) {
	t := test.New(tt)

	e := Event{"specversion": "0.2", "contenttype": "application/foo"}
	t.ExpectEqual("0.2", e.SpecVersion())
	t.ExpectEqual("application/foo", e.ContentType())

	e = Event{"cloudEventsVersion": "0.1", "contentType": "application/bar"}
	t.ExpectEqual("0.1", e.SpecVersion())
	t.ExpectEqual("application/bar", e.ContentType())

	// Should ignore content-type attribute from wrong version
	e = Event{"cloudEventsVersion": "0.1", "contenttype": "application/bar"}
	t.ExpectEqual("", e.ContentType())
	e = Event{"specversion": "0.2", "contentType": "application/bar"}
	t.ExpectEqual("", e.ContentType())

	e = Event{"specversion": "0.3", "contentType": "a", "contenttype": "b"}
	t.ExpectEqual("b", e.ContentType())
}

func TestEventDataValue(tt *testing.T) {
	t := test.New(tt)

	e := Event{"data": 1}
	t.ExpectEqual(1, e["data"])
	v, err := e.DataValue()
	t.ExpectEqual(err, nil)
	t.ExpectEqual(1, v)

	e = Event{"data": []byte{42}}
	v, err = e.DataValue()
	t.ExpectEqual(err, nil)
	t.ExpectEqual(v, []byte{42})

	e = Event{"data": bytes.NewReader([]byte{32})}
	v, err = e.DataValue()
	t.ExpectEqual(err, nil)
	t.ExpectEqual(v, []byte{32})
}

func TestEventDataReader(tt *testing.T) {
	t := test.New(tt)

	e := Event{"data": 1}
	t.ExpectEqual(1, e["data"])
	r := e.DataReader()
	t.ExpectEqual(nil, r)

	e = Event{"data": []byte{42}}
	r = e.DataReader()
	v, err := ioutil.ReadAll(r.(io.Reader))
	t.ExpectEqual(nil, err)
	t.ExpectEqual([]byte{42}, v)

	e = Event{"data": bytes.NewReader([]byte{32})}
	r = e.DataReader()
	v, err = ioutil.ReadAll(r.(io.Reader))
	t.ExpectEqual([]byte{32}, v)
}

func TestEventFormat(tt *testing.T) {
	t := test.New(tt)

	e := Event{"data": "hello", "specversion": "2.0", "contenttype": "text/plain"}
	s, err := e.Format(JSONFormat)
	t.ExpectNil(err)
	t.ExpectEqual("application/cloudevents+json", s.Format.Name())
	b, err := ioutil.ReadAll(s.Reader)
	t.ExpectNil(err)
	t.ExpectEqual(`{"contenttype":"text/plain","data":"hello","specversion":"2.0"}`, string(b))
	// Make a new structured event since we've consumed the data in this one
	s, err = e.Format(JSONFormat)
	e2, err := s.Event()
	t.ExpectNil(err)
	t.ExpectEqual(e, e2)
}
