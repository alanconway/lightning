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

func TestJSONFormat(tt *testing.T) {
	t := test.New(tt)
	e := Event{"data": "foo", "specversion": "2.0"}
	b, err := JSONFormat.Marshal(e)
	t.ExpectEqual(nil, err)
	t.ExpectEqual("{\"data\":\"foo\",\"specversion\":\"2.0\"}", string(b))

	var e2 Event
	err = JSONFormat.Unmarshal(b, &e2)
	t.ExpectEqual(nil, err)

	t.ExpectEqual("foo", e2["data"])
	sv := e2.SpecVersion()
	t.ExpectEqual(nil, err)
	t.ExpectEqual("2.0", sv)

	t.ExpectEqual(e, e2)
}
