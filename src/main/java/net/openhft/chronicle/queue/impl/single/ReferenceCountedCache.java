package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.ReferenceOwner;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Thread-safe, self-cleaning cache for ReferenceCounted objects.
 * <p>
 * Created by Jerry Shea on 27/04/18.
 */
public class ReferenceCountedCache<K, T extends ReferenceCounted, V, E extends Throwable> implements ReferenceOwner {
    private final Map<K, T> cache = new ConcurrentHashMap<>();
    private final Function<T, V> transformer;
    private final ThrowingFunction<K, T, E> creator;

    public ReferenceCountedCache(Function<T, V> transformer, ThrowingFunction<K, T, E> creator) {
        this.transformer = transformer;
        this.creator = creator;
    }

    @NotNull
    V get(@NotNull final K key) throws E {

        // remove all which have been dereferenced. Garbagey but rare
        cache.entrySet().removeIf(entry -> entry.getValue().refCount() == 0);

        @Nullable T value = cache.get(key);

        // another thread may have reduced refCount since removeIf above
        if (value == null || !value.tryReserve(this)) {
            // worst case is that 2 threads create at 'same' time
            value = creator.apply(key);
            cache.put(key, value);
        }

        return transformer.apply(value);
    }
}
