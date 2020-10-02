Apache Aries OSGi Type Safe Events
-------------------------------------------------------------

This project contains an implementation of the OSGi Typed Safe Event Service specification, and related services to allow the Type Safe Event Service to interoperate with remote event sources and consumers.

The Type Safe Event Service is defined in Chapter 157 of the OSGi R7 specification. This specification is not yet final, but public drafts of this specification are available from the OSGi Alliance.

Given that the specification is non-final the OSGi API declared in this project is subject to change at any time up to its official release. Also the behaviour of this implementation may not always be up-to-date with the latest wording in the specification . The project maintainers will, however try to keep pace with the specification, and to ensure that the implementations remain compliant with any relevant OSGi specifications.

# Modules

The following modules are available for use in OSGi

1. org.apache.aries.typedevent.bus :- This project contains the implementations of the OSGi Type Safe Event Service.
2. org.apache.aries.typedevent.remote :- This reactor project is the home for "Remote Event" implementations which allow the Typed Event Service to interoperate with remote systems.

## Which modules should I use?

If you're looking at this project then you almost certainly want to use org.apache.aries.typedevent.bus. If you also want to support distributed events then you should also take a look at the various remote event implementations present