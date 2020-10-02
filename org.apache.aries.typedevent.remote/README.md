Apache Aries OSGi Type Safe Event Service Remote Integration
-------------------------------------------------------------

This reactor project contains modules which integrate OSGi Typed Safe Events with remote event sources
and remote event consumers.

Over time it is expected that more modules will be added, each providing remote access using a different technology. 

## Common code

All modules are expected to conform to the common API rules defined by this project. Namely that

* A marker property is added to the untyped event data, indicating that the event is from a remote source (see the constants class for details)
* A RemoteEventMonitor (which is aware of whether events are from a remote source)
* A service property that listeners can advertise, to indicate whether their topic/filter should be considered for remote events.
