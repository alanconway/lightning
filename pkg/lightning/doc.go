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

/*
Package lighting provides interfaces and types to build multi-format,
multi-protocol event sources and sinks. Lightning connects the clouds.

WARNING UNSTABLE EXPERIMENTAL CODE

Features

Adapters are separated into independent Source and Sink.  Any
Source/Sink implementations can be combined.

Built-in conversion between binary/structured messages and multiple
structured event formats independent of Source/Sink
implementations.

Works with cloudEvents 0.1 and 0.2 (and with future specs provided
the specversion, contenttype and data attributes are not rename
again)

Forwards structured messages without decoding and re-encoding if
both Source and Sink accept the format.

TODO

Experimental code, needs more testing and review.

https:github.com/cloudevents/spec/issues/261 cloudEvent spec is
unclear about when json data is base64 encoded.  Currently we make
an educated guess on the contenttype but this needs work.

QoS: protocols like HTTP, AMQP, MQTT can provide varied QoS. To
support at-least-once or exactly-once the sink needs to notify sthe
source. Source and sink should also advertise their QoS range so both
sides can degrade to the lowest level for performance, since the
adapter's overall QoS will be that of the weakest link anyway.

Performance: Current Source & Sink impls do more copying than
they should, no performance benchmarks have been done to date.
*/
package lightning
