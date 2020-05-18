package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.CommonStore;
import net.openhft.chronicle.queue.impl.StoreReleasable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class ClosableResources<T extends StoreReleasable> {
    @NotNull
    private final T storeReleasable;
    private final AtomicBoolean released = new AtomicBoolean();
    private AtomicReference<Wire> wireReference = new AtomicReference<>();
    private AtomicReference<Wire> wireForIndexReference = new AtomicReference<>();
    private AtomicReference<CommonStore> storeReference = new AtomicReference<>();

    private ClosableResources(@NotNull final T storeReleasable) {
        this.storeReleasable = storeReleasable;
    }

    public synchronized void releaseResources() {
        if (released.compareAndSet(false, true)) {
            wireReference(null).storeReference(null).wireForIndexReference(null);
        }
    }

    public ClosableResources wireForIndexReference(final Wire ref) {
        Wire oldRef = this.wireForIndexReference.getAndSet(ref);
        if (oldRef != null)
            StoreComponentReferenceHandler.queueForRelease(oldRef);
        return this;
    }

    public ClosableResources wireReference(final Wire ref) {
        Wire oldRef = this.wireReference.getAndSet(ref);
        if (oldRef != null)
            StoreComponentReferenceHandler.queueForRelease(oldRef);
        return this;
    }

    public ClosableResources storeReference(final CommonStore ref) {
        CommonStore oldRef = this.storeReference.getAndSet(ref);
        if (oldRef != null)
            storeReleasable.release(oldRef);
        return this;
    }
}