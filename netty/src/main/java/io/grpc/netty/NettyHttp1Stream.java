package io.grpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpConversionUtil;
import java.io.IOException;
import java.net.SocketAddress;
import javax.net.ssl.SSLSession;

/**
 * An abstraction that allows writing to an HTTP/1 stream. This mimics {@link NettyHttp2Stream}, but
 * has some key differences due to how Netty implements HTTP/1 and HTTP/2 differently.
 *
 * <p>Calls to this class must be made in a specific order: {@link #writeHeaders}, {@link
 * #writeData} (may be called multiple times) and finally {@link #writeData} with {@code
 * endStream=true}. However, if the call to {@link #writeHeaders} passes a {@link FullHttpResponse},
 * then no further calls must be made.
 */
public final class NettyHttp1Stream implements NettyHttpStream {
  private final SSLSession session;
  private final Channel channel;
  private NettyHttpStreamListener listener;

  public NettyHttp1Stream(Channel channel, SSLSession session) {
    this.session = session;
    this.channel = channel;
  }

  @Override
  public SSLSession getSSLSession() {
    return session;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return channel.remoteAddress();
  }

  /**
   * Sets a listener to receive onReady and onCancel events; incoming requests are never streamed,
   * so the {@code dataReceived} method on this listener is never called.
   */
  @Override
  public void setListener(NettyHttpStreamListener listener) {
    this.listener = listener;
  }

  void onCancel() {
    NettyHttpStreamListener handler = listener;
    if (handler != null) {
      handler.onCancel();
    }
  }

  void receiveData(ByteBuf data, boolean endOfStream) {
    this.listener.dataReceived(data, endOfStream);
  }

  @Override
  public void writeHeaders(Http2Headers headers) throws IOException {
    try {
      writeHeaders(HttpConversionUtil.toHttpResponse(0, headers, true));
    } catch (Http2Exception e) {
      // I don't want to leak any more netty details to the caller.
      throw new IOException(e);
    }
  }

  public void writeHeaders(HttpResponse response) {
    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    channel
        .eventLoop()
        .execute(
            () -> {
              if (response instanceof FullHttpResponse) {
                channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
              } else {
                channel.write(response);
              }
            });
  }

  @Override
  public void writeData(byte[] data, boolean endStream) {
    ByteBuf buf = Unpooled.copiedBuffer(data);
    // We apparently flush after every write, also for HTTP/2. We have tests that fail if we don't.
    writeData(buf, endStream, true);
  }

  public void writeData(ByteBuf bytebuf, boolean endStream, boolean flush) {
    channel
        .eventLoop()
        .execute(
            () -> {
              DefaultHttpContent content = new DefaultHttpContent(bytebuf);
              if (endStream) {
                channel.writeAndFlush(content).addListener(ChannelFutureListener.CLOSE);
              } else if (flush) {
                channel.writeAndFlush(content);
              } else {
                channel.write(content);
              }
            });
  }

  /**
   * Closes the channel. This may or may not flush existing data in the channel that hasn't been
   * sent yet. Also, HTTP/1 doesn't provide a way to notify the client that an error has happened
   * while streaming. Use sparingly.
   */
  @Override
  public void cancel() {
    channel.close();
  }

  @Override
  public boolean isReady() {
    return true;
  }
}
