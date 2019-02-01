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

func TestEventData(tt *testing.T) {
	t := test.New(tt)

	e := Event{"data": 1}
	t.ExpectEqual(1, e["data"])
	t.ExpectEqual(1, e.Data())

	e = Event{"data": []byte{42}}
	t.ExpectEqual(e.Data(), []byte{42})

	e = Event{"data": []byte{32}}
	t.ExpectEqual(e.Data(), []byte{32})
}

func TestEventFormat(tt *testing.T) {
	t := test.New(tt)

	e := Event{"data": "hello", "specversion": "2.0", "contenttype": "text/plain"}
	s, err := e.Format(JSONFormat)
	t.ExpectNil(err)
	t.ExpectEqual("application/cloudevents+json", s.Format.Name())
	t.ExpectEqual(`{"contenttype":"text/plain","data":"hello","specversion":"2.0"}`, string(s.Bytes))
	// Make a new structured event since we've consumed the data in this one
	s, err = e.Format(JSONFormat)
	e2, err := s.Event()
	t.ExpectNil(err)
	t.ExpectEqual(e, e2)
}
