/*
 * Copyright 2016-2020 chronicle.software
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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceOwner;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.queue.impl.CommonStore;
import net.openhft.chronicle.queue.impl.StoreReleasable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"ConstantConditions", "rawtypes"})
public class SingleChronicleQueueExcerpts {
    static final boolean CHECK_INDEX = Boolean.getBoolean("queue.check.index");
    static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);
    static final int MESSAGE_HISTORY_METHOD_ID = -1;
    static final StringBuilderPool SBP = new StringBuilderPool();

    static void releaseWireResources(final Wire wire) {
        StoreComponentReferenceHandler.queueForRelease(wire);
    }

    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    static void releaseIfNotNullAndReferenced(@Nullable final Bytes bytesReference, ReferenceOwner id) {
        if (bytesReference != null) {
            try {
                bytesReference.release(id);
            } catch (IllegalStateException e) {
                Jvm.warn().on(SingleChronicleQueueExcerpts.class, e);
            }
        }
    }

    /**
     * please don't use this interface as its an internal implementation.
     */
    public interface InternalAppender {
        void writeBytes(long index, BytesStore bytes);
    }

// *************************************************************************
//
// TAILERS
//
// *************************************************************************

    static final class ClosableResources<T extends StoreReleasable> {
        private final ReferenceOwner owner;
        @NotNull
        private final T storeReleasable;
        private final AtomicBoolean released = new AtomicBoolean();
        volatile Bytes bytesReference = null;
        volatile Bytes bytesForIndexReference = null;
        volatile CommonStore storeReference = null;
        @Nullable Pretoucher pretoucher = null;


        ClosableResources(ReferenceOwner owner, @NotNull final T storeReleasable) {
            this.owner = owner;
            this.storeReleasable = storeReleasable;
        }

        private static void releaseIfNotNull(final Bytes bytesReference, ReferenceOwner id) {
            // Object is no longer reachable
            if (bytesReference != null) {
                bytesReference.release(id);
            }
        }

        void releaseResources() {
            if (released.getAndSet(true)) {
                return;
            }
            releaseIfNotNull(bytesReference, owner);

            // Object is no longer reachable, check that it has not already been released
            if (storeReference != null) {
                storeReleasable.release(owner, storeReference);
            }
            Closeable.closeQuietly(pretoucher);
            pretoucher = null;
            storeReference = null;
        }

        public void checkReleased() {
            if (!released.get())
                throw new IllegalStateException("Not released");
        }
    }

}