package org.superbiz.hazelcast;

import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import org.apache.openejb.OpenEJBRuntimeException;
import org.apache.openejb.core.stateful.Cache;
import org.apache.openejb.core.stateful.Instance;
import org.apache.openejb.util.Duration;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HazelcastCache<A> implements Cache<A, Instance> {
    public static final String LOCK_PREFIX = "tomee-stateful-lock-";

    private static final ConcurrentMap<String, HazelcastInstance> instances = new ConcurrentHashMap<String, HazelcastInstance>();

    // xbean config
    private String cacheId = HazelcastCache.class.getName(); // if multiple stateful containers
    private String hazelcastXml = HazelcastCache.class.getResource("/default-openejb-hazelcast.xml").toExternalForm();
    private String hazelcastMapName = "tomee-stateful-cache";
    private Properties properties = new Properties();

    // internal state
    private CacheListener<Instance> listener;
    private HazelcastInstance instance;
    private IMap<A, StatefulEntry<A>> cache;
    private String listenerId = null;

    @Override
    public CacheListener<Instance> getListener() {
        return listener;
    }

    @Override
    public void setListener(final CacheListener<Instance> listener) {
        this.listener = listener;
        addListener();
    }

    @Override
    public void add(final A key, final Instance value) {
        StatefulEntry<A> entry = cache.get(key);
        if (entry != null) {
            final ILock lock = entry.getLock();
            lock.lock();
            try {
                if (entry.getState() != StatefulEntry.EntryState.REMOVED) {
                    throw new IllegalStateException("An entry for the key " + key + " already exists");
                }
                cache.remove(key);
            } finally {
                lock.unlock();
            }
        }

        final String id = value.beanContext.getId();
        entry = new StatefulEntry(cacheId, id, key, value, StatefulEntry.EntryState.CHECKED_OUT, instance.getLock(lockName(id)));
        setWithTimeout(key, entry);
    }

    private void setWithTimeout(final A key, final StatefulEntry<A> entry) {
        final Duration duration = entry.getInternalValue().getTimeOut();
        if (duration != null) {
            cache.set(key, entry, duration.getTime(), duration.getUnit());
        } else {
            cache.set(key, entry);
        }
    }

    @Override
    public Instance checkOut(final A key, final boolean loadIfNotFound) throws Exception {
        for (int i = 0; i < 10; i++) {
            final StatefulEntry<A> entry = cache.get(key);
            if (!loadIfNotFound && entry == null) {
                return null;
            }
            if (entry == null) {
                // no need of any passivator since hazelcast is in memory oriented
                // so it wouldn't make sense
                return null;
            }

            entry.getLock().lock();
            try {
                // verfiy state
                switch (entry.getState()) {
                    case AVAILABLE:
                        break;
                    case CHECKED_OUT:
                        setWithTimeout(key, entry);
                        return entry.getValue();
                    case PASSIVATED:
                        cache.remove(key, entry);
                        continue;
                    case REMOVED:
                        return null;
                }

                entry.setState(StatefulEntry.EntryState.CHECKED_OUT);
                return entry.getValue();
            } finally {
                entry.getLock().unlock();
            }
        }

        // something is really messed up with this entry, try to cleanup before throwing an exception
        final StatefulEntry<A> entry = cache.remove(key);
        if (entry != null) {
            entry.setState(StatefulEntry.EntryState.REMOVED);
        }
        throw new OpenEJBRuntimeException("Cache is corrupted: the entry " + key + " in the Map 'cache' is in state PASSIVATED");
    }

    @Override
    public void checkIn(final A key) {
        final StatefulEntry<A> entry = cache.get(key);
        if (entry == null) {
            return;
        }

        final ILock lock = entry.getLock();
        lock.lock();
        try {
            switch (entry.getState()) {
                case PASSIVATED:
                    throw new IllegalStateException("The entry " + key + " is not checked-out");
                default:
                    setWithTimeout(key, entry);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Instance remove(final A key) {
        final StatefulEntry<A> entry = cache.get(key);
        if (entry == null) {
            return null;
        }

        final ILock lock = entry.getLock();
        lock.lock();
        try {
            cache.remove(key);
            entry.setState(StatefulEntry.EntryState.REMOVED);
            return entry.getValue();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void removeAll(final CacheFilter<Instance> filter) {
        for (final Iterator<StatefulEntry<A>> iterator = cache.values().iterator(); iterator.hasNext();) {
            final StatefulEntry<A> entry = iterator.next();
            final ILock lock = entry.getLock();
            lock.lock();
            try {
                if (filter.matches(entry.getValue())) {
                    iterator.remove();
                    entry.setState(StatefulEntry.EntryState.REMOVED);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void init() {
        instance = Hazelcast.getHazelcastInstanceByName(hazelcastXml);
        if (instance == null) {
            try {
                instance = Hazelcast.newHazelcastInstance(new UrlXmlConfig(hazelcastXml, properties));
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }
        cache = instance.getMap(hazelcastMapName);
        addListener();

        instances.putIfAbsent(cacheId, instance);
    }

    private void addListener() {
        if (listener == null) { // when init is called 99% we don't have it
            return;
        }
        if (listenerId != null) {
            cache.removeEntryListener(listenerId);
        }
        listenerId = cache.addEntryListener(new StatefulEntryListener<A>(listener), true);
    }

    @Override
    public void destroy() {
        instances.remove(cacheId);
        instance.getLifecycleService().shutdown();
    }

    public void setHazelcastXml(final String hazelcastXml) {
        this.hazelcastXml = hazelcastXml;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public void setCacheId(final String cacheId) {
        this.cacheId = cacheId;
    }

    public void setHazelcastMapName(final String hazelcastMapName) {
        this.hazelcastMapName = hazelcastMapName;
    }

    public static HazelcastInstance getInstance(final String id) {
        return instances.get(id);
    }

    public static String lockName(final String id) {
        return LOCK_PREFIX + id;
    }
}
