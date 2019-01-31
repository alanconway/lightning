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
	"testing"

	"github.com/alanconway/lightning/internal/pkg/test"
)

func TestJSONBytesData(t *testing.T) {
	tb := test.New(t)

	e := Event{"specversion": "2.0", "data": []byte{1, 2, 3}, "contenttype": "application/octet-stream"}
	b, err := JSONFormat.Marshal(e)
	tb.ExpectEqual([]byte{1, 2, 3}, e["data"]) // Don't modify in marshal
	tb.ExpectEqual(nil, err)
	// "AQID" is base64 encoding of binary 0x010203
	tb.ExpectEqual(`{"contenttype":"application/octet-stream","data":"AQID","specversion":"2.0"}`, string(b))

	var e2 Event
	tb.ExpectNil(JSONFormat.Unmarshal(b, &e2))
	tb.ExpectEqual(e, e2)
	tb.ExpectEqual(e2["data"], []byte{1, 2, 3}) // unmarshalled data never base64 encoded
}

func TestJSONReaderData(t *testing.T) {
	tb := test.New(t)

	e := Event{"specversion": "2.0", "data": []byte{1, 2, 3}, "contenttype": "application/octet-stream"}
	b, err := JSONFormat.Marshal(e)
	tb.ExpectEqual([]byte{1, 2, 3}, e["data"]) // Marshal reads the bytes
	tb.ExpectEqual(nil, err)
	// "AQID" is base64 encoding of binary 0x010203
	tb.ExpectEqual(`{"contenttype":"application/octet-stream","data":"AQID","specversion":"2.0"}`, string(b))

	var e2 Event
	tb.ExpectNil(JSONFormat.Unmarshal(b, &e2))
	tb.ExpectEqual(e, e2)
	tb.ExpectEqual(e2["data"], []byte{1, 2, 3}) // unmarshalled data never base64 encoded
}

func TestJSONStringNoContent(tt *testing.T) {
	t := test.New(tt)
	e := Event{"data": "foo", "specversion": "2.0"}
	b, err := JSONFormat.Marshal(e)
	t.ExpectEqual(nil, err)
	t.ExpectEqual("{\"data\":\"foo\",\"specversion\":\"2.0\"}", string(b))

	var e2 Event
	err = JSONFormat.Unmarshal(b, &e2)
	t.ExpectNil(err)
	t.ExpectEqual(e, e2)
}

func TestJSONStringContent(tt *testing.T) {
	t := test.New(tt)
	e := Event{"contenttype": "text/plain", "data": "foo", "specversion": "2.0"}
	b, err := JSONFormat.Marshal(e)
	t.ExpectEqual(nil, err)
	var e2 Event
	err = JSONFormat.Unmarshal(b, &e2)
	t.ExpectNil(err)
	t.ExpectEqual(e, e2)
}
