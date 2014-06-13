hazelcast-tomee-poc
===================

Hazelcast prototype to implement stateful clustering

Here is the expected configuration (tomee.xml format):

```
<Container id="statefuls" type="STATEFUL">
    cache = org.superbiz.hazelcast.HazelcastCache
    lockFactory = org.superbiz.hazelcast.HazelcastLockFactory

    # optional but otherwise it will prevent statefuls with extended EM to be distributed
    # Side note: this is maybe not a good idea
    preventExtendedEntityManagerSerialization = false
</Container>
```
