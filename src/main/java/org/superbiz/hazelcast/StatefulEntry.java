package org.superbiz.hazelcast;

import com.hazelcast.core.ILock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.openejb.core.stateful.Instance;

import java.io.IOException;

public class StatefulEntry<K> implements DataSerializable {
    public enum EntryState {
        AVAILABLE, CHECKED_OUT, PASSIVATED, REMOVED
    }

    private String cacheId;
    private String instanceId;
    private ILock lock;
    private K key;
    private Instance value;
    private EntryState state;

    public StatefulEntry() {
        // no-op: for serialization
    }

    public StatefulEntry(final String cacheId, final String instanceId, final K key, final Instance value, final EntryState state, final ILock lock) {
        this.cacheId = cacheId;
        this.instanceId = instanceId;
        this.key = key;
        this.value = value;
        this.state = state;
        this.lock = lock;
    }

    public K getKey() {
        assertLockHeld();
        return key;
    }

    public Instance getValue() {
        assertLockHeld();
        return value;
    }

    public EntryState getState() {
        assertLockHeld();
        return state;
    }

    public void setState(final EntryState state) {
        assertLockHeld();
        this.state = state;
    }

    private void assertLockHeld() {
        if (!lock.isLockedByCurrentThread()) {
            throw new IllegalStateException("Entry must be locked");
        }
    }

    public ILock getLock() {
        return lock;
    }

    public Instance getInternalValue() {
        return value;
    }

    @Override
    public void writeData(final ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(instanceId);
        objectDataOutput.writeUTF(cacheId);
        objectDataOutput.writeUTF(lock.getName());
        objectDataOutput.writeObject(key);
        objectDataOutput.writeObject(value);
        objectDataOutput.writeObject(state);
    }

    @Override
    public void readData(final ObjectDataInput objectDataInput) throws IOException {
        instanceId = objectDataInput.readUTF();
        cacheId = objectDataInput.readUTF();
        final String lockName = objectDataInput.readUTF();
        lock = HazelcastCache.getInstance(cacheId).getLock(lockName);
        key = objectDataInput.readObject();
        value = objectDataInput.readObject();
        state = objectDataInput.readObject();
    }
}
