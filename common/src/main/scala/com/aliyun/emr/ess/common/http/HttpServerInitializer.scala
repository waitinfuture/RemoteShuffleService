package com.aliyun.emr.ess.common.http

import io.netty.channel.{ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpServerCodec}

class HttpServerInitializer(handlers: SimpleChannelInboundHandler[_]) extends ChannelInitializer[SocketChannel] {

  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpServerCodec())
      .addLast("httpAggregator", new HttpObjectAggregator(512 * 1024))
        .addLast(handlers)
  }
}
