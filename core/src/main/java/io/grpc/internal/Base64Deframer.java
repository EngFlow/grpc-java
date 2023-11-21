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
import io.grpc.Status;
import java.util.Base64;

final class Base64Deframer implements Deframer {
  private final Deframer delegate;
  private final byte[] buffer = new byte[4];
  private int bufferedBytes;

  public Base64Deframer(MessageDeframer.Listener listener, MessageDeframer delegate) {
    this.delegate = delegate;
    delegate.setListener(
        new ForwardingDeframerListener() {
          @Override
          protected MessageDeframer.Listener delegate() {
            return listener;
          }

          @Override
          public void deframerClosed(boolean partialMessage) {
            super.deframerClosed(partialMessage || bufferedBytes > 0);
          }
        });
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
    try {
      int fill = Math.min(4 - bufferedBytes, data.readableBytes());
      data.readBytes(buffer, bufferedBytes, fill);
      bufferedBytes += fill;
      if (bufferedBytes < 4) {
        return;
      }
      delegate.deframe(ReadableBuffers.wrap(decodeBase64(buffer)));
      int decodable = data.readableBytes() & ~3;
      byte[] raw = new byte[decodable];
      data.readBytes(raw, 0, decodable);
      byte[] decoded = decodeBase64(raw);
      delegate.deframe(ReadableBuffers.wrap(decoded));
      bufferedBytes = data.readableBytes();
      data.readBytes(buffer, 0, bufferedBytes);
    } finally {
      data.close();
    }
  }

  private static byte[] decodeBase64(byte[] input) {
    try {
      return Base64.getDecoder().decode(input);
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("gRPC-web-text base64 decoding failed: " + e.getMessage())
          .withCause(e)
          .asRuntimeException();
    }
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
