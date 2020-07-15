package com.aliyun.emr.ess.service.deploy.master.http

import java.net.InetSocketAddress

import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.service.deploy.master.Master
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LoggingHandler, LogLevel}

class HttpServer(var port: Int, master: Master) extends Logging {
  @throws[Exception]
  def start(): Unit = {
    val bootstrap = new ServerBootstrap
    val boss = new NioEventLoopGroup(2)
    val work = new NioEventLoopGroup(2)

    bootstrap.group(boss, work).
      handler(new LoggingHandler(LogLevel.DEBUG)).
      channel(classOf[NioServerSocketChannel]).
      childHandler(new HttpServerInitializer(master))

    val f = bootstrap.bind(new InetSocketAddress(port)).sync
    logInfo(s"HttpServer start on port ${port}")
    f.syncUninterruptibly()
  }
}