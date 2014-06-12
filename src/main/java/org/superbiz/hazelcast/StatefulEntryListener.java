package org.superbiz.hazelcast;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ILock;
import org.apache.openejb.core.stateful.Cache;
import org.apache.openejb.core.stateful.Instance;

public class StatefulEntryListener<K> implements EntryListener<K, StatefulEntry<K>> {
    private final Cache.CacheListener<Instance> delegate;

    public StatefulEntryListener(final Cache.CacheListener<Instance> listener) {
        delegate = listener;
    }

    @Override
    public void entryAdded(final EntryEvent<K, StatefulEntry<K>> kStatefulEntryEntryEvent) {
        // no-op
    }

    @Override
    public void entryRemoved(final EntryEvent<K, StatefulEntry<K>> entry) {
        // no-op
    }

    @Override
    public void entryUpdated(final EntryEvent<K, StatefulEntry<K>> kStatefulEntryEntryEvent) {
        // no-op
    }

    @Override
    public void entryEvicted(final EntryEvent<K, StatefulEntry<K>> entry) {
        final StatefulEntry<K> value = entry.getValue();
        if (value != null) {
            final ILock lock = value.getLock();
            lock.lock();
            try {
                delegate.timedOut(value.getValue());
            } finally {
                lock.unlock();
            }
        }
    }
}
