Apache Aries Type Safe Events over OSGi Remote Services
-------------------------------------------------------------

This project uses OSGi remote services to provide inter-framework integration between OSGi Typed Safe Event Services.

## How does it work

Each instance registers a `RemoteEventBus` service with a service property indicating the event topic(s) and filter(s) for events that it is interested in. 

Each instance consumes the `RemoteEventBus` instances from other nodes and creates `UntypedEventHandler` instances for each topic/filter that the remote nodes have advertised interest in.

Events can then be routed from the local framework to a remote framework by passing the events to the relevant `RemoteEventBus`. When a `RemoteEventBus` receives an event it then publishes it locally using the `TypedEventBus` 


## Usage

This module should work automatically when a Remote Services provider is running.

For topic/filters in the local framework to be considered as "remotable" the service should add the `RemoteEventConstants.RECEIVE_REMOTE_EVENTS` property with a value of true. Otherwise this module must be configured to select more local listeners using the PID `org.apache.aries.typedevent.remote.remoteservices`.
