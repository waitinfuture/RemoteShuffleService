package com.aliyun.emr.ess.service.deploy.master.http

import com.aliyun.emr.ess.service.deploy.master.Master
import io.netty.channel.socket.SocketChannel
import io.netty.channel.ChannelInitializer
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpServerCodec}

class HttpServerInitializer(master: Master) extends ChannelInitializer[SocketChannel] {

  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpServerCodec())
      .addLast("httpAggregator", new HttpObjectAggregator(512 * 1024))
      .addLast(new HttpRequestHandler(master))
  }
}
