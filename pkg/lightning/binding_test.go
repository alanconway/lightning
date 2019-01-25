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
	"strings"
	"testing"

	"github.com/alanconway/lightning/internal/pkg/test"
	"go.uber.org/zap"
)

type dummySource struct{ Number int }

func (*dummySource) Incoming() <-chan Message { return nil }
func (*dummySource) Close()                   {}
func (*dummySource) Run() error               { return nil }

type dummySink struct{ SinkStr string }

func (*dummySink) Close()       {}
func (*dummySink) Send(Message) {}

type dummyBinding struct{}

func (dummyBinding) Source(c Config) (Source, error) {
	s := dummySource{}
	err := c.Unmarshal(&s)
	return &s, err
}
func (dummyBinding) Sink(c Config) (Sink, error) {
	s := dummySink{}
	return &s, c.Unmarshal(&s)
}
func (dummyBinding) Name() string          { return "dummy" }
func (dummyBinding) Doc() string           { return "dummy doc" }
func (dummyBinding) SetLogger(*zap.Logger) {}

var bindings = NewBindings(dummyBinding{})

func TestBindingSource(tt *testing.T) {
	t := test.New(tt)
	s, err := bindings.Source([]byte(`{"binding": "dummy", "number": 12}`))
	t.ExpectNil(err)
	t.ExpectEqual(&dummySource{Number: 12}, s)
}

func TestConfigurationSink(tt *testing.T) {
	t := test.New(tt)
	s, err := bindings.Sink([]byte(`{"binding": "dummy", "sinkstr": "foo"}`))
	t.ExpectNil(err)
	t.ExpectEqual(&dummySink{SinkStr: "foo"}, s)
}

func TestBadConfiguration(tt *testing.T) {
	t := test.New(tt)

	_, err := bindings.Source([]byte(`{"binding": "xxx", "number": 12}`))
	t.ExpectMatch(`No such binding: "xxx"`, err.Error())

	_, err = bindings.Source([]byte(`{"binding": "dummy", "number": "nan"}`))
	t.Require(err != nil, "expected error ")

	_, err = bindings.Source([]byte("{bad}"))
	t.ExpectMatch("invalid character", err.Error())
}

func TestGetConfig(tt *testing.T) {
	t := test.New(tt)

	c, err := MakeConfig("testdata/dummy-config.json")
	t.ExpectNil(err)
	t.ExpectEqual(`{"binding": "dummy", "number": 42}`, strings.Trim(c.String(), "\n"))

	c, err = MakeConfig(`{"binding": "dummy", "number": 99}`)
	t.ExpectNil(err)
	t.ExpectEqual(`{"binding": "dummy", "number": 99}`, c.String())
}
