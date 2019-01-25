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
)

func TestMediaTypeIsBinary(t *testing.T) {
	for _, m := range []string{"", "text", "text/foobar", "application/blah+xml", "application/javascript", "application/json", "application/foo-html", "application/foo.html.blah", "application/json-seq"} {
		if MediaTypeIsBinary(m) {
			t.Errorf("expected !MediaTypeIsBinary(%#v)", m)
		}
	}
	for _, m := range []string{"audo/foo", "xyz", "application/blah+blah", "application/x"} {
		if !MediaTypeIsBinary(m) {
			t.Errorf("expected MediaTypeIsText(%#v)", m)
		}
	}
}
