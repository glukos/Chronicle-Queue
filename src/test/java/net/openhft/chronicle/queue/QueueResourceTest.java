/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.OS;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueueResourceTest {
    @Test
    public void testResources() {
        String tmp = OS.TMP + "/testResources-" + System.nanoTime();
        ExcerptAppender appender;
        ExcerptTailer tailer;
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmp)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {
            appender = queue.acquireAppender();
            appender.writeText("Hello World");
            tailer = queue.createTailer();
            assertEquals("Hello World", tailer.readText());
        }
        appender.checkReleased();
        tailer.checkReleased();
        MappedFile.checkMappedFiles();
    }
}
