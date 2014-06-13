package org.superbiz.hazelcast;

import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.openejb.jee.EjbJar;
import org.apache.openejb.jee.EnterpriseBean;
import org.apache.openejb.jee.StatefulBean;
import org.apache.openejb.junit.ApplicationComposer;
import org.apache.openejb.testing.AppResource;
import org.apache.openejb.testing.Classes;
import org.apache.openejb.testing.Configuration;
import org.apache.openejb.testing.Module;
import org.apache.openejb.testng.PropertiesBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.PreDestroy;
import javax.ejb.AccessTimeout;
import javax.ejb.Remove;
import javax.ejb.Stateful;
import javax.ejb.StatefulTimeout;
import javax.naming.Context;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(ApplicationComposer.class)
public class HazelcastStatefulTest {
    @Module
    @Classes(HzStateful.class)
    public EjbJar stateful() {
        return new EjbJar();
    }

    @Configuration
    public Properties configuration() {
        return new PropertiesBuilder()
                .p("statefuls", "new://Container?type=STATEFUL")
                .p("statefuls.cache", "org.superbiz.hazelcast.HazelcastCache")
                .p("statefuls.lockFactory", "org.superbiz.hazelcast.HazelcastLockFactory")
                .p("statefuls.preventExtendedEntityManagerSerialization", "false")
                .build();
    }

    @AppResource
    private Context ctx;

    @Test
    public void businessMethod() throws NamingException, IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(new UrlXmlConfig(HazelcastCache.class.getResource("/default-openejb-hazelcast.xml")));
        final IMap<Object, Object> map = instance.getMap("tomee-stateful-cache");

        final String jndi = "java:global/HazelcastStatefulTest/stateful/HzStateful";
        final HzStateful stateful = HzStateful.class.cast(ctx.lookup(jndi));
        assertEquals(1, map.size());
        assertNotNull(stateful);
        assertEquals("Hazelcast", stateful.computeName());
        assertEquals(1, map.size());

        stateful.remove();
        assertTrue(map.isEmpty());

        instance.getLifecycleService().shutdown();
    }

    @Test
    public void timeout() throws NamingException, IOException, InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(new UrlXmlConfig(HazelcastCache.class.getResource("/default-openejb-hazelcast.xml")));
        final IMap<Object, Object> map = instance.getMap("tomee-stateful-cache");

        final String jndi = "java:global/HazelcastStatefulTest/stateful/HzStateful";
        final HzStateful stateful = HzStateful.class.cast(ctx.lookup(jndi));
        assertEquals(1, map.size());
        assertNotNull(stateful);
        assertEquals("Hazelcast", stateful.computeName());

        assertFalse(HzStateful.timeout);
        sleep(2500);
        assertTrue(HzStateful.timeout);

        assertTrue(map.isEmpty());

        instance.getLifecycleService().shutdown();
    }

    @Stateful
    @StatefulTimeout(value = 1, unit = TimeUnit.SECONDS)
    public static class HzStateful {
        private static boolean timeout = false;

        public String computeName() {
            return "Hazelcast";
        }

        @PreDestroy
        public void timeout() {
            timeout = true;
        }

        @Remove
        public void remove() {}
    }
}
