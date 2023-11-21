package io.grpc.netty;

import io.netty.buffer.ByteBuf;

/**
 * A listener for incoming HTTP content, excluding headers, as well as notification that the stream
 * is ready to receive more data or that the incoming call was cancelled.
 */
public interface NettyHttpStreamListener {
  void dataReceived(ByteBuf buf, boolean endOfStream);

  void onReady();

  void onCancel();
}
