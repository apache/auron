/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.jni;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

public class JniBridgeTest {

    @Test
    public void testFileWrappersPreserveLiteralHashInHdfsPath() throws Exception {
        String path = "hdfs://mycluster/tmp/channel=wx_repro#mini/spark-submit-123/part-00000.json";
        CapturingFileSystem cfs = new CapturingFileSystem();

        JniBridge.openFileAsDataInputWrapper(cfs, path).close();
        JniBridge.createFileAsDataOutputWrapper(cfs, path).close();

        assertPathPreservesHash(cfs.openedPath);
        assertPathPreservesHash(cfs.createdPath);
    }

    @Test
    public void testFileWrappersPreserveNormalizedPercentPathStrings() throws Exception {
        String path = "file:/tmp/t1/part=test%20test/part-00000.parquet";
        CapturingFileSystem cfs = new CapturingFileSystem();

        JniBridge.openFileAsDataInputWrapper(cfs, path).close();
        JniBridge.createFileAsDataOutputWrapper(cfs, path).close();

        assertEquals(
                "/tmp/t1/part=test%20test/part-00000.parquet",
                cfs.openedPath.toUri().getPath());
        assertEquals(
                "/tmp/t1/part=test%20test/part-00000.parquet",
                cfs.createdPath.toUri().getPath());
    }

    @Test
    public void testHadoopPathUriAcceptsFilePathWithSpace() throws Exception {
        String path = "file:/tmp/t1/part=test test/part-00000.parquet";

        assertThrows(URISyntaxException.class, () -> new URI(path));
        assertEquals("file", new Path(path).toUri().getScheme());
        assertEquals(
                "/tmp/t1/part=test test/part-00000.parquet",
                new Path(path).toUri().getPath());
    }

    private static void assertPathPreservesHash(Path path) {
        assertEquals(
                "/tmp/channel=wx_repro#mini/spark-submit-123/part-00000.json",
                path.toUri().getPath());
        assertNull(path.toUri().getFragment());
    }

    private static class CapturingFileSystem extends RawLocalFileSystem {
        private final Statistics statistics = new Statistics("hdfs");
        private Path openedPath;
        private Path createdPath;

        @Override
        public FSDataInputStream open(Path path) throws IOException {
            openedPath = path;
            return new FSDataInputStream(new EmptyFSInputStream());
        }

        @Override
        public FSDataOutputStream create(Path path) throws IOException {
            createdPath = path;
            return new FSDataOutputStream(new ByteArrayOutputStream(), statistics);
        }
    }

    private static class EmptyFSInputStream extends FSInputStream {
        @Override
        public void seek(long pos) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read() {
            return -1;
        }
    }
}
