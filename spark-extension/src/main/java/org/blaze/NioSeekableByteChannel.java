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

package org.blaze;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class NioSeekableByteChannel implements SeekableByteChannel {
  private ByteBuffer buffer;
  private long offset;

  public NioSeekableByteChannel(ByteBuffer buffer, long offset, long length) {
    this.buffer = buffer.duplicate();
    this.offset = offset;

    this.buffer.position((int) offset);
    this.buffer.limit((int) (offset + length));
  }

  @Override
  public int read(ByteBuffer dest) throws IOException {
    if (buffer.position() == buffer.limit()) {
      return -1;
    }
    int readSize = Math.min(dest.capacity() - dest.position(), buffer.limit() - buffer.position());

    ByteBuffer bufferPart = buffer.duplicate();
    bufferPart.limit(bufferPart.position() + readSize);

    ByteBuffer destPart = dest.duplicate();
    destPart.limit(readSize);

    destPart.put(bufferPart);
    dest.position(dest.position() + readSize);
    buffer.position(buffer.position() + readSize);
    return readSize;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    this.buffer.position((int) (this.offset + newPosition));
    return this;
  }

  @Override
  public long position() throws IOException {
    return (long) this.buffer.position() - this.offset;
  }

  @Override
  public long size() throws IOException {
    return (long) this.buffer.limit() - this.offset;
  }

  @Override
  public boolean isOpen() {
    return this.buffer != null;
  }

  @Override
  public void close() throws IOException {
    this.buffer = null;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Read only");
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Read only");
  }
}
