package io.grpc.netty;

import io.netty.buffer.ByteBuf;

public interface NettyHttpStreamListener {
  void dataReceived(ByteBuf buf, boolean endOfStream);
}
