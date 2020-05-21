/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceOwner;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.queue.TailerDirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.ParseException;
import java.util.NavigableSet;

public class WireStorePool implements StoreReleasable {
    @NotNull
    private final WireStoreSupplier supplier;
    private final StoreFileListener storeFileListener;
    private boolean isClosed = false;
    private StackTrace closedHere;

    private WireStorePool(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        this.supplier = supplier;
        this.storeFileListener = storeFileListener;
    }

    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        return new WireStorePool(supplier, storeFileListener);
    }

    public void close() {
        if (isClosed)
            return;
        isClosed = true;
        closedHere = Jvm.isReferenceTracing() ? new StackTrace() : null;
    }

    @Nullable
    public WireStore acquire(ReferenceOwner owner, final int cycle, final long epoch, boolean createIfAbsent) {
        if (isClosed)
            throw new IllegalStateException("Closed", closedHere);
        WireStore store = this.supplier.acquire(owner, cycle, createIfAbsent);
        if (store != null)
            storeFileListener.onAcquired(cycle, store.file());
        return store;
    }

    public int nextCycle(final int currentCycle, @NotNull TailerDirection direction) throws ParseException {
        return supplier.nextCycle(currentCycle, direction);
    }

    @Override
    public void release(ReferenceOwner ro, @NotNull CommonStore store) {
        store.release(ro);
        if (store instanceof WireStore) {
            WireStore wireStore = (WireStore) store;
            if (storeFileListener != null)
                storeFileListener.onReleased(wireStore.cycle(), store.file());
        }
    }

    /**
     * list cycles between ( inclusive )
     *
     * @param lowerCycle the lower cycle
     * @param upperCycle the upper cycle
     * @return an array including these cycles and all the intermediate cycles
     */
    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) throws ParseException {
        return supplier.cycles(lowerCycle, upperCycle);
    }
}
