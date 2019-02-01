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
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
)

// IsRefused returns true for "connection refused" errors that warrant
// a reconnect attempt. A convenience for implementing connection retry
// in bindings.
func IsRefused(err error) bool {
	for {
		switch err := err.(type) {
		case *net.OpError:
			return err.Op == "read" || strings.Contains(err.Error(), "connection refused")
		case syscall.Errno:
			return err == syscall.ECONNREFUSED
		default:
			return false
		}
	}
}

var id int64

// UniqueID returns a unique (within this process) ID string beginning with s
func UniqueID(s string) string { return fmt.Sprintf("%v%v", s, atomic.AddInt64(&id, 1)) }
