package io.grpc.netty;

import io.netty.handler.codec.http2.Http2Headers;

/**
 * Implementations of this interface can intercept incoming HTTP/2 streams that are *not* gRPC
 * calls and reply to them however they see fit (within the constraints of the HTTP/2 protocol).
 */
public interface HttpStreamListener {
  void startStream(NettyHttpStream stream, Http2Headers headers);
}
