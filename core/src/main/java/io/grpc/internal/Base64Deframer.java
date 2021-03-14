/*
 * Copyright 2021 The gRPC Authors
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

package io.grpc.internal;

import io.grpc.Decompressor;
import java.util.Base64;

public class Base64Deframer implements Deframer {
  private final Deframer delegate;

  public Base64Deframer(Deframer delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setMaxInboundMessageSize(int messageSize) {
    delegate.setMaxInboundMessageSize(messageSize);
  }

  @Override
  public void setDecompressor(Decompressor decompressor) {
    delegate.setDecompressor(decompressor);
  }

  @Override
  public void setFullStreamDecompressor(GzipInflatingBuffer fullStreamDecompressor) {
    delegate.setFullStreamDecompressor(fullStreamDecompressor);
  }

  @Override
  public void request(int numMessages) {
    delegate.request(numMessages);
  }

  @Override
  public void deframe(ReadableBuffer data) {
    // TODO: Make this more efficient.
    byte[] decoded = Base64.getDecoder().decode(ReadableBuffers.readArray(data));
    data.close();
    delegate.deframe(ReadableBuffers.wrap(decoded));
  }

  @Override
  public void closeWhenComplete() {
    delegate.closeWhenComplete();
  }

  @Override
  public void close() {
    delegate.close();
  }
}
