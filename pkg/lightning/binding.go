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
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// Binding creates related Source and Sink objects, and
// manages configuration and logging.
type Binding interface {
	// Name of the binding
	Name() string
	// Source creates a new source, configured with config.
	Source(Config) (Source, error)
	// Sink creates a new sink, configured with config.
	Sink(Config) (Sink, error)
	// Doc returns binding documentation.
	Doc() string
	// SetLogger() sets the logger for the binding.
	SetLogger(*zap.Logger)
}

// Bindings is a map of registered bindings by name.
// It holds a root logger that is set on all bindings.
type Bindings struct {
	Log *zap.Logger
	m   map[string]Binding
}

// NewBindings creates a new Bindings map
func NewBindings(b ...Binding) *Bindings {
	bs := &Bindings{m: make(map[string]Binding), Log: zap.NewNop()}
	for _, x := range b {
		bs.Add(x)
	}
	return bs
}

// Names returns the names of all registered bindings
func (bs *Bindings) Names() (names []string) {
	for k, _ := range bs.m {
		names = append(names, k)
	}
	return
}

// Add adds bindings to the map
func (bs *Bindings) Add(b Binding) {
	bs.m[b.Name()] = b
	b.SetLogger(bs.Log.Named(b.Name()))
}

// Get gets a binding by name, returns nil if not found
func (bs *Bindings) Get(name string) Binding {
	return bs.m[name]
}

func (bs *Bindings) missing(name string) string {
	return fmt.Sprintf("No such binding: %#v", name)
}

// Doc returns a documentation string for the named binding
func (bs *Bindings) Doc(name string) string {
	if b := bs.Get(name); b != nil {
		return b.Doc()
	} else {
		return bs.missing(name)
	}
}

// Set the root Log for all binding
func (bs *Bindings) SetLogger(log *zap.Logger) {
	bs.Log = log
	for _, b := range bs.m {
		bs.Add(b) // Re-Add to set the Log
	}
}

func (bs *Bindings) get(c Config) (Binding, error) {
	var cc CommonConfig
	err := c.Unmarshal(&cc)
	if err == nil {
		if b := bs.Get(cc.Binding); b != nil {
			return b, nil
		}
		err = errors.New(bs.missing(cc.Binding))
	}
	return nil, err
}

// Source configures a new source using the "binding" name in the configuration.
func (bs *Bindings) Source(c Config) (s Source, err error) {
	var b Binding
	b, err = bs.get(c)
	if err == nil {
		s, err = b.Source(c)
		if err == nil {
			bs.Log.Info("Created source", zap.ByteString("config", c.Bytes()))
			return
		}
	}
	bs.Log.Error("Error creating source", zap.Error(err), zap.ByteString("config", c.Bytes()))
	return
}

// Sink configures a new sink using the "binding" name in the configuration.
func (bs *Bindings) Sink(c Config) (s Sink, err error) {
	var b Binding
	b, err = bs.get(c)
	if err == nil {
		s, err = b.Sink(c)
		if err == nil {
			bs.Log.Info("Created sink", zap.ByteString("config", c.Bytes()))
			return
		}
	}
	bs.Log.Error("Error creating sink", zap.Error(err), zap.ByteString("config", c.Bytes()))
	return
}
