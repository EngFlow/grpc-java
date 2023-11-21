package io.grpc.netty;

import io.netty.handler.codec.http2.Http2Headers;
import java.io.IOException;
import java.net.SocketAddress;
import javax.net.ssl.SSLSession;

/**
 * Writes a response to an outgoing HTTP stream. Calls to this class must be made in a specific
 * order: {@code writeHeaders}, {@code writeData} (may be called multiple times) and finally {@code
 * writeData} with {@code endStream=true}.
 *
 * <p>If something goes wrong, call {@code cancel}: if the outgoing stream is an HTTP/2 stream, it
 * will be closed with a generic error code, if it is an HTTP/1 stream, it will just be closed.
 */
public interface NettyHttpStream {
  SSLSession getSSLSession();

  SocketAddress getRemoteAddress();

  void setListener(NettyHttpStreamListener listener);

  void writeHeaders(Http2Headers headers) throws IOException;

  void writeData(byte[] data, boolean endStream);

  void cancel();

  boolean isReady();
}
