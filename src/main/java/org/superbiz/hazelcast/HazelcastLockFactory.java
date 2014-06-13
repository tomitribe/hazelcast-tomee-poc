package org.superbiz.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import org.apache.openejb.core.stateful.LockFactory;
import org.apache.openejb.core.stateful.StatefulContainer;

import java.util.concurrent.TimeUnit;

public class HazelcastLockFactory implements LockFactory {
    private HazelcastInstance hazelcastInstance;

    @Override
    public StatefulLock newLock(final String s) {
        return new HazelcastLock(hazelcastInstance.getLock("tomee-stateful-instance-locks-" + s));
    }

    @Override
    public void setContainer(final StatefulContainer statefulContainer) {
        hazelcastInstance = HazelcastCache.class.cast(statefulContainer.getCache()).getInstance();
    }

    public static class HazelcastLock implements StatefulLock {
        private final ILock delegate;

        public HazelcastLock(final ILock lock) {
            this.delegate = lock;
        }

        @Override
        public void lock() {
            delegate.lock();
        }

        @Override
        public void unlock() {
            delegate.unlock();
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return delegate.isLockedByCurrentThread();
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            return delegate.tryLock(l, timeUnit);
        }
    }
}
