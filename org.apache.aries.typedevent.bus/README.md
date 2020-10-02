Apache Aries OSGi Type Safe Event Service implementation
-------------------------------------------------------------

This project contains an implementation of the OSGi Typed Safe Event Service specification.

The Type Safe Event Service is defined in Chapter 157 of the OSGi R7 specification. This specification is not yet final, but public drafts of this specification are available from the OSGi Alliance.

Given that the specification is non-final the OSGi API declared in this project is subject to change at any time up to its official release. Also the behaviour of this implementation may not always be up-to-date with the latest wording in the specification . The project maintainers will, however try to keep pace with the specification, and to ensure that the implementations remain compliant with any relevant OSGi specifications.

## Usage

When started this bundle registers a `TypedEvenBus` service which users can use to send events. Events can be received by registering a `TypedEventHandler` or `UntypedEventHandler` whiteboard service.

Events flowing through the system can be monitored with the `TypedEventMonitor` service, which is also registered at startup. This implementation offers limited history playback.

The configuration PID for the Aries implementation is `org.apache.aries.typedevent.bus`. Currently there are no configuration properties
