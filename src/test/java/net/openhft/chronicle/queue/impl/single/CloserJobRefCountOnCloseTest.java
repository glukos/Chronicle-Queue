/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Test;

public class CloserJobRefCountOnCloseTest {

    /**
     * @see <a>https://github.com/OpenHFT/Chronicle-Queue/issues/664</a>
     */
    @Test
    public void test() {
        try (SingleChronicleQueue temp = SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("temp")).build()) {
            temp.acquireAppender()
                    .writeText("hello");

            ExcerptTailer tailer = temp.createTailer();
            String s = tailer.readText();
            if (tailer instanceof StoreTailer) {
                final StoreTailer storeTailer = (StoreTailer) tailer;
                storeTailer.close();
            }
            ((Runnable) tailer::close).run();
        }
    }
}
