**NOTE UNSTABLE EXPERIMENTAL CODE**

# Lightning connects the clouds

Lightning is a multi-protocol, multi-format library for building
cloud-event sources and sinks.

## What's different about lightning?

The cloud-event specification provides multiple ways to represent
events: as structured JSON, or as binary content AMQP or MQTT messages
for example. The set of possible formats and protocols is open-ended.

There are already libraries for sending and receiving cloud events
using HTTP and JSON, but they tend to support JSON and HTTP only, with
no public hooks to extend to other formats or protocols.

There are knative event sources for converting events from non-HTTP
protocols to JSON/HTTP but they tend to be specific to a single
non-HTTP protocol and usually only support JSON format.

Lightning does the following:

* Implements common conversions between structured and binary messages

* Manages an extendable set of Formats, automatically Marshals/Unmarshals
  structured events correctly based on the format media type.

* Separates the implementation Sources and Sinks.  Sources and Sinks
  for arbitrary protocols can be mixed and matched.


## Summary of packages

Package [lightning](https://godoc.org/github.com/alanconway/lightning/pkg/lightning) provides an abstract Event, extensible Format conversions, and Source/Sink interfaces to help build adapters.

Package [amqp](https://godoc.org/github.com/alanconway/lightning/pkg/amqp) provies AMQP sources and sinks.

Package [mqtt](https://godoc.org/github.com/alanconway/lightning/pkg/amqp) provides an MQTT 3.1.1 sources and sinks.

Package [http](https://godoc.org/github.com/alanconway/lightning/pkg/http) provides HTTP sources and sinks.



