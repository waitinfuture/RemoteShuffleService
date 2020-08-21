package com.aliyun.emr.ess.common.http

import java.net.InetSocketAddress

import com.aliyun.emr.ess.common.internal.Logging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LogLevel, LoggingHandler}

class HttpServer(channelInitializer: ChannelInitializer[_], port: Int) extends Logging {
  @throws[Exception]
  def start(): Unit = {
    val bootstrap = new ServerBootstrap
    val boss = new NioEventLoopGroup(2)
    val work = new NioEventLoopGroup(2)

    bootstrap.group(boss, work).
      handler(new LoggingHandler(LogLevel.DEBUG)).
      channel(classOf[NioServerSocketChannel]).
      childHandler(channelInitializer)

    val f = bootstrap.bind(new InetSocketAddress(port)).sync
    logInfo(s"HttpServer start on port ${port}")
    f.syncUninterruptibly()
  }
}