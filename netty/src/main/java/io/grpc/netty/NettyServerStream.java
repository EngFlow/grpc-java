/*
 * Copyright 2014 The gRPC Authors
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

package io.grpc.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.Protocol;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.WritableBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.base64.Base64Dialect;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.perfmark.Link;
import io.perfmark.PerfMark;
import io.perfmark.Tag;
import io.perfmark.TaskCloseable;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server stream for a Netty HTTP2 transport. Must only be called from the sending application
 * thread.
 */
class NettyServerStream extends AbstractServerStream {
  private static final Logger log = Logger.getLogger(NettyServerStream.class.getName());

  private final Sink sink;
  private final TransportState state;
  private final Channel channel;
  private final WriteQueue writeQueue;
  private final Attributes attributes;
  private final String authority;
  private final TransportTracer transportTracer;
  private final int streamId;
  private final Protocol protocol;

  public NettyServerStream(
      Channel channel,
      TransportState state,
      Attributes transportAttrs,
      String authority,
      StatsTraceContext statsTraceCtx,
      TransportTracer transportTracer,
      Protocol protocol) {
    super(new NettyWritableBufferAllocator(channel.alloc()), statsTraceCtx);
    this.sink =
        protocol == Protocol.GRPC_WEB_TEXT
            ? new GrpcWebTextSink()
            : protocol == Protocol.GRPC_WEB_PROTO ? new GrpcWebProtoSink() : new Sink();
    this.state = checkNotNull(state, "transportState");
    this.channel = checkNotNull(channel, "channel");
    this.writeQueue = state.handler.getWriteQueue();
    this.attributes = checkNotNull(transportAttrs);
    this.authority = authority;
    this.transportTracer = checkNotNull(transportTracer, "transportTracer");
    // Read the id early to avoid reading transportState later.
    this.streamId = transportState().id();
    this.protocol = protocol;
  }

  @Override
  protected TransportState transportState() {
    return state;
  }

  @Override
  protected Sink abstractServerStreamSink() {
    return sink;
  }

  @Override
  public Attributes getAttributes() {
    return attributes;
  }

  @Override
  public String getAuthority() {
    return authority;
  }

  private class Sink implements AbstractServerStream.Sink {
    @Override
    public void writeHeaders(Metadata headers) {
      try (TaskCloseable ignore = PerfMark.traceTask("NettyServerStream$Sink.writeHeaders")) {
        writeQueue.enqueue(
            SendResponseHeadersCommand.createHeaders(
                transportState(), Utils.convertServerHeaders(headers, protocol)),
            true);
      }
    }

    protected void writeFrameInternal(ByteBuf bytebuf, boolean flush, final int numMessages) {
      Preconditions.checkArgument(numMessages >= 0);
      final int numBytes = bytebuf.readableBytes();
      // Add the bytes to outbound flow control.
      onSendingBytes(numBytes);
      writeQueue
          .enqueue(new SendGrpcFrameCommand(transportState(), bytebuf, false), flush)
          .addListener(
              new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  // Remove the bytes from outbound flow control, optionally notifying
                  // the client that they can send more bytes.
                  transportState().onSentBytes(numBytes);
                  if (future.isSuccess()) {
                    transportTracer.reportMessageSent(numMessages);
                  }
                }
              });
    }

    @Override
    public void writeFrame(WritableBuffer frame, boolean flush, final int numMessages) {
      try (TaskCloseable ignore = PerfMark.traceTask("NettyServerStream$Sink.writeFrame")) {
        if (frame == null) {
          writeQueue.scheduleFlush();
          return;
        }
        ByteBuf bytebuf = ((NettyWritableBuffer) frame).bytebuf().touch();
        writeFrameInternal(bytebuf, flush, numMessages);
      }
    }

    @Override
    public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
      try (TaskCloseable ignore = PerfMark.traceTask("NettyServerStream$Sink.writeTrailers")) {
        Http2Headers http2Trailers = Utils.convertTrailers(trailers, headersSent);
        writeQueue.enqueue(
            SendResponseHeadersCommand.createTrailers(transportState(), http2Trailers, status),
            true);
      }
    }

    @Override
    public void cancel(Status status) {
      try (TaskCloseable ignore = PerfMark.traceTask("NettyServerStream$Sink.cancel")) {
        writeQueue.enqueue(new CancelServerStreamCommand(transportState(), status), true);
      }
    }
  }

  private class GrpcWebProtoSink extends Sink implements AbstractServerStream.Sink {
    @Override
    public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
      // The grpc-web protocol is special in that it cannot use existing HTTP/2 facilities to post
      // trailers. Instead, we send them as headers here and *also* as trailer frames.
      if (!headersSent) {
        // Note that writeHeaders automatically takes grpc-web into account by rewriting the given
        // headers.
        writeHeaders(trailers);
      }
      PerfMark.startTask("NettyServerStream$Sink.writeTrailers");
      try {
        // TODO: convertTrailers does not take the protocol into account. What to do?
        Http2Headers http2Trailers = Utils.convertTrailers(trailers, true);

        // TODO: What's the right size to use here?
        ByteBuf bytebuf = channel.alloc().buffer(1024).touch();
        // The trailers are sent as a base64-encoded packet with the following structure:
        // byte ID(0x80)
        // int length (total length of the HTTP headers not including the ID and length fields)
        // HTTP headers as a list of <string>:<value>\r\n
        bytebuf.writeByte(0x80); // TRAILER FRAME
        // We don't know the size yet so we write 0 and patch it afterwards.
        int address = bytebuf.writerIndex();
        bytebuf.writeInt(0);

        // We use ISO_8859_1 for the Charset to match Netty. See HpackEncoder.encodeStringLiteral.
        for (Entry<CharSequence, CharSequence> entry : http2Trailers) {
          bytebuf.writeCharSequence(
              String.format("%s:%s\r\n", entry.getKey(), entry.getValue()),
              StandardCharsets.ISO_8859_1);
        }

        // Patch the length.
        int len = bytebuf.readableBytes() - 5;
        bytebuf.setByte(address + 0, (len >> 24) & 0xff);
        bytebuf.setByte(address + 1, (len >> 16) & 0xff);
        bytebuf.setByte(address + 2, (len >> 8) & 0xff);
        bytebuf.setByte(address + 3, len & 0xff);

        int numBytes = bytebuf.readableBytes();
        writeQueue
            .enqueue(new SendGrpcFrameCommand(transportState(), bytebuf, true), true)
            .addListener(
                new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                    // TODO: I think we need to update flow control here. IS THAT CORRECT?
                    // Remove the bytes from outbound flow control, optionally notifying
                    // the client that they can send more bytes.
                    transportState().onSentBytes(numBytes);
                    // TODO: I think this isn't needed, but not sure. Check with upstream!
                    //                      if (future.isSuccess()) {
                    //                        transportTracer.reportMessageSent(numMessages);
                    //                      }
                  }
                });
      } finally {
        PerfMark.stopTask("NettyServerStream$Sink.writeTrailers");
      }
    }
  }

  // TODO: remove when we're no longer using the grpc-web-text protocol anywhere
  private class GrpcWebTextSink extends Sink implements AbstractServerStream.Sink {
    @Override
    protected void writeFrameInternal(ByteBuf unencoded, boolean flush, final int numMessages) {
      // The frames already have the correct header (0x00 plus 4-byte length); we only need to
      // encode them as Base64.
      ByteBuf encoded =
          Base64.encode(
                  unencoded,
                  unencoded.readerIndex(),
                  unencoded.readableBytes(),
                  false,
                  Base64Dialect.STANDARD,
                  channel.alloc())
              .touch();
      unencoded.release();
      super.writeFrameInternal(encoded, flush, numMessages);
    }

    @Override
    public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
      // The grpc-web protocol is special in that it cannot use existing HTTP/2 facilities to post
      // trailers. Instead, we send them as headers here and *also* as trailer frames.
      if (!headersSent) {
        // Note that writeHeaders automatically takes grpc-web into account by rewriting the given
        // headers.
        writeHeaders(trailers);
      }
      PerfMark.startTask("NettyServerStream$Sink.writeTrailers");
      try {
        // TODO: convertTrailers does not take the protocol into account. What to do?
        Http2Headers http2Trailers = Utils.convertTrailers(trailers, true);

        // TODO: What's the right size to use here?
        ByteBuf unencoded = channel.alloc().buffer(1024).touch();
        // The trailers are sent as a base64-encoded packet with the following structure:
        // byte ID(0x80)
        // int length (total length of the HTTP headers not including the ID and length fields)
        // HTTP headers as a list of <string>:<value>\r\n
        unencoded.writeByte(0x80); // TRAILER FRAME
        // We don't know the size yet so we write 0 and patch it afterwards.
        int address = unencoded.writerIndex();
        unencoded.writeInt(0);

        // We use ISO_8859_1 for the Charset to match Netty. See HpackEncoder.encodeStringLiteral.
        for (Entry<CharSequence, CharSequence> entry : http2Trailers) {
          unencoded.writeCharSequence(
              String.format("%s:%s\r\n", entry.getKey(), entry.getValue()),
              StandardCharsets.ISO_8859_1);
        }

        // Patch the length.
        int len = unencoded.readableBytes() - 5;
        unencoded.setByte(address + 0, (len >> 24) & 0xff);
        unencoded.setByte(address + 1, (len >> 16) & 0xff);
        unencoded.setByte(address + 2, (len >> 8) & 0xff);
        unencoded.setByte(address + 3, len & 0xff);

        ByteBuf bytebuf =
            Base64.encode(
                    unencoded,
                    unencoded.readerIndex(),
                    unencoded.readableBytes(),
                    false,
                    Base64Dialect.STANDARD,
                    channel.alloc())
                .touch();
        unencoded.release();

        int numBytes = bytebuf.readableBytes();
        writeQueue
            .enqueue(new SendGrpcFrameCommand(transportState(), bytebuf, true), true)
            .addListener(
                new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                    // TODO: I think we need to update flow control here. IS THAT CORRECT?
                    // Remove the bytes from outbound flow control, optionally notifying
                    // the client that they can send more bytes.
                    transportState().onSentBytes(numBytes);
                    // TODO: I think this isn't needed, but not sure. Check with upstream!
                    //                      if (future.isSuccess()) {
                    //                        transportTracer.reportMessageSent(numMessages);
                    //                      }
                  }
                });
      } finally {
        PerfMark.stopTask("NettyServerStream$Sink.writeTrailers");
      }
    }
  }

  /** This should only be called from the transport thread. */
  public static class TransportState extends AbstractServerStream.TransportState
      implements StreamIdHolder {
    private final Http2Stream http2Stream;
    private final NettyServerHandler handler;
    private final EventLoop eventLoop;
    private final Tag tag;

    public TransportState(
        NettyServerHandler handler,
        EventLoop eventLoop,
        Http2Stream http2Stream,
        int maxMessageSize,
        StatsTraceContext statsTraceCtx,
        TransportTracer transportTracer,
        String methodName,
        Protocol protocol) {
      super(maxMessageSize, statsTraceCtx, transportTracer, protocol);
      this.http2Stream = checkNotNull(http2Stream, "http2Stream");
      this.handler = checkNotNull(handler, "handler");
      this.eventLoop = eventLoop;
      this.tag = PerfMark.createTag(methodName, http2Stream.id());
    }

    @Override
    public void runOnTransportThread(final Runnable r) {
      if (eventLoop.inEventLoop()) {
        r.run();
      } else {
        final Link link = PerfMark.linkOut();
        eventLoop.execute(
            new Runnable() {
              @Override
              public void run() {
                try (TaskCloseable ignore =
                    PerfMark.traceTask("NettyServerStream$TransportState.runOnTransportThread")) {
                  PerfMark.attachTag(tag);
                  PerfMark.linkIn(link);
                  r.run();
                }
              }
            });
      }
    }

    @Override
    public void bytesRead(int processedBytes) {
      handler.returnProcessedBytes(http2Stream, processedBytes);
      handler.getWriteQueue().scheduleFlush();
    }

    @Override
    public void deframeFailed(Throwable cause) {
      log.log(Level.WARNING, "Exception processing message", cause);
      Status status = Status.fromThrowable(cause);
      transportReportStatus(status);
      handler.getWriteQueue().enqueue(new CancelServerStreamCommand(this, status), true);
    }

    void inboundDataReceived(ByteBuf frame, boolean endOfStream) {
      super.inboundDataReceived(new NettyReadableBuffer(frame.retain()), endOfStream);
    }

    @Override
    public int id() {
      return http2Stream.id();
    }

    @Override
    public Tag tag() {
      return tag;
    }
  }

  @Override
  public int streamId() {
    return streamId;
  }
}
