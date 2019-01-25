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
	"regexp"
)

var nonBinary *regexp.Regexp

func init() {
	sep := "[/+-.]"
	re := "(^$)|(^text$)|(^text/)|(^application.*" +
		sep + "(xml|json|yaml|javascript|html|text|wbxml)" + "(" + sep + "|$))"
	nonBinary = regexp.MustCompile(re)
}

// MediaTypeIsBinary returns true if the media type is guessed to have
// a binary encoding. Used for mappings like JSON that base64-encode
// binary data.
//
// See https://github.com/cloudevents/spec/issues/261#issuecomment-448650135
func MediaTypeIsBinary(mediaType string) bool {
	return !nonBinary.Match([]byte(mediaType))
}
