package io.grpc.netty;

import io.grpc.Status;
import io.grpc.internal.ServerStreamListener;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.WritableBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.perfmark.PerfMark;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstraction that allows writing to an HTTP/2 stream. Calls to this class must be made in a
 * specific order: {@code writeHeaders}, {@code writeData} (may be called multiple times) and
 * finally {@code writeData} with {@code endStream=true}.
 *
 * <p>If something goes wrong, call {@code cancel} to close the HTTP/2 stream with a generic error
 * code.
 */
public class NettyHttpStream {
  public static AtomicReference<HttpStreamListener> httpStreamListener = new AtomicReference<>();

  private final TransportState state;
  private final WriteQueue writeQueue;
  private final NettyWritableBufferAllocator allocator;

  public NettyHttpStream(Channel channel, TransportState state, NettyServerHandler handler) {
    this.state = state;
    this.writeQueue = handler.getWriteQueue();
    this.allocator = new NettyWritableBufferAllocator(channel.alloc());
  }

  public void setListener(ServerStreamListener listener) {
    throw new UnsupportedOperationException();
  }

  public void setListener(NettyHttpStreamListener listener) {
    state.setListener(listener);
  }

  public void writeHeaders(Http2Headers headers) {
    PerfMark.startTask("NettyHttpStream.writeHeaders");
    try {
      writeQueue.enqueue(SendResponseHeadersCommand.createHeaders(state, headers), false);
    } finally {
      PerfMark.stopTask("NettyHttpStream.writeHeaders");
    }
  }

  public void writeData(byte[] data, boolean endStream) {
    int bytesWritten = 0;
    while (bytesWritten < data.length) {
      WritableBuffer buffer = allocator.allocate(data.length - bytesWritten);
      int len = Math.min(buffer.writableBytes(), data.length - bytesWritten);
      buffer.write(data, bytesWritten, len);
      bytesWritten += len;
      writeData(
          ((NettyWritableBuffer) buffer).bytebuf(),
          false,
          bytesWritten == data.length && !endStream);
    }
    if (endStream) {
      WritableBuffer buffer = allocator.allocate(0);
      writeData(((NettyWritableBuffer) buffer).bytebuf(), true, true);
    }
  }

  public void writeData(ByteBuf bytebuf, boolean endStream, boolean flush) {
    if (endStream) {
      state.setClosedStatus(Status.OK);
    }
    final int numBytes = bytebuf.readableBytes();
    // Add the bytes to outbound flow control.
    state.onSendingBytes(numBytes);
    writeQueue
        .enqueue(new SendGrpcFrameCommand(state, bytebuf, true), flush)
        .addListener(
            new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture future) throws Exception {
                // Remove the bytes from outbound flow control, optionally notifying
                // the client that they can send more bytes.
                state.onSentBytes(numBytes);
              }
            });
  }

  public void cancel() {
    PerfMark.startTask("NettyHttpStream.cancel");
    try {
      writeQueue.enqueue(new CancelServerStreamCommand(state, Status.ABORTED), true);
    } finally {
      PerfMark.startTask("NettyHttpStream.cancel");
    }
  }

  private static class NoOpServerStreamListenerImpl implements ServerStreamListener {
    NoOpServerStreamListenerImpl() {}

    @Override
    public void halfClosed() {}

    @Override
    public void closed(Status status) {}

    @Override
    public void messagesAvailable(MessageProducer producer) {}

    @Override
    public void onReady() {}
  }

  public static class TransportState extends NettyServerStream.TransportState
      implements StreamIdHolder {
    private NettyHttpStreamListener listener;

    public TransportState(
        NettyServerHandler handler,
        EventLoop eventLoop,
        Http2Stream http2Stream,
        int maxMessageSize,
        StatsTraceContext statsTraceCtx,
        TransportTracer transportTracer,
        String methodName) {
      super(
          handler,
          eventLoop,
          http2Stream,
          maxMessageSize,
          statsTraceCtx,
          transportTracer,
          methodName);
    }

    void setListener(NettyHttpStreamListener listener) {
      this.listener = listener;
      setListener(new NoOpServerStreamListenerImpl());
    }

    @Override
    void inboundDataReceived(ByteBuf frame, boolean endOfStream) {
      listener.dataReceived(frame, endOfStream);
    }

    // We need to call this method on the great-grandparent, where it is unfortunately declared
    // private.
    // There is a protected method on AbstractStream, but we don't want to extend AbstractStream as
    // that brings in a lot of gRPC-specific code.
    void onSendingBytes(int numBytes) {
      try {
        Method m =
            TransportState.class
                .getSuperclass()
                .getSuperclass()
                .getSuperclass()
                .getDeclaredMethod("onSendingBytes", int.class);
        m.setAccessible(true);
        m.invoke(this, numBytes);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    // We need to call this method on the grandparent, where it is unfortunately declared private.
    void setClosedStatus(Status status) {
      try {
        Method m =
            TransportState.class
                .getSuperclass()
                .getSuperclass()
                .getDeclaredMethod("setClosedStatus", Status.class);
        m.setAccessible(true);
        m.invoke(this, status);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
