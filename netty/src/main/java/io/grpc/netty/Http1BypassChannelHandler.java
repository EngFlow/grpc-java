package io.grpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

class Http1BypassChannelHandler extends ChannelDuplexHandler {
  private static final Logger log = Logger.getLogger(Http1BypassChannelHandler.class.getName());
  private static final int MAX_CONTENT_LENGTH = 1024 * 1024;

  @Nullable private final HttpStreamListener listener;
  private NettyHttp1Stream stream;

  Http1BypassChannelHandler(HttpStreamListener listener) {
    this.listener = listener;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);

    ctx.pipeline().addBefore(ctx.name(), null, new HttpServerCodec());

    // Aggregate *incoming* requests, not outgoing responses (this is unclear from the docs, but I
    // found a discussion here: https://github.com/netty/netty/discussions/11828).
    ctx.pipeline().addBefore(ctx.name(), null, new HttpObjectAggregator(MAX_CONTENT_LENGTH));

    // Remove the WriteBufferingAndExceptionHandler instance that is added in NettyServerTransport
    // after the protocol negotiator, and which makes it impossible to send outgoing HTTP/1 packets
    // via the channel.
    ctx.pipeline().removeLast();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (stream != null) {
      stream.onCancel();
      stream = null;
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    try {
      if (!(object instanceof FullHttpRequest)) {
        return;
      }
      FullHttpRequest request = (FullHttpRequest) object;

      if (listener != null) {
        Channel channel = ctx.channel();
        // We currently only support HTTP/1 over TLS + protocol negotiation.
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        NettyHttp1Stream stream = new NettyHttp1Stream(channel, sslHandler.engine().getSession());
        this.stream = stream;
        // We need a SCHEME to generate the Http/2 headers, otherwise the toHttp2Headers call
        // throws. Fortunately, the HttpConversionUtil allow us to pass it in this way.
        request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "https");
        // We need to set Content-Length so the listener knows whether to expect content or not.
        ByteBuf content = request.content();
        request
            .headers()
            .add(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(content.readableBytes()));
        listener.startStream(stream, HttpConversionUtil.toHttp2Headers(request, false));
        // If the Content-Length is 0, then don't call receiveData, for consistency with HTTP/2.
        if (content.readableBytes() != 0) {
          stream.receiveData(content, true);
        }
        return;
      }

      FullHttpResponse response = createProtocolUnsupportedResponse(ctx, request);
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } finally {
      ReferenceCountUtil.release(object);
    }
  }

  private FullHttpResponse createProtocolUnsupportedResponse(
      ChannelHandlerContext ctx, FullHttpRequest request) {
    ByteBuf content = ctx.alloc().buffer();
    ByteBufUtil.writeAscii(
        content, String.format("Received unsupported '%s' request\n", request.protocolVersion()));
    FullHttpResponse response =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, content);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    return response;
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    this.stream = null;
    super.close(ctx, promise);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    log.log(Level.SEVERE, "Caught exception in HTTP/1.1 mode", cause);
    ctx.close();
  }
}
