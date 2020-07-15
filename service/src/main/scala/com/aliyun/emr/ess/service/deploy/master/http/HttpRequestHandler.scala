package com.aliyun.emr.ess.service.deploy.master.http

import com.aliyun.emr.ess.service.deploy.master.Master
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, FullHttpRequest, HttpHeaderNames, HttpResponseStatus, HttpVersion}
import io.netty.util.CharsetUtil

class HttpRequestHandler(val master: Master) extends SimpleChannelInboundHandler[FullHttpRequest] {
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.uri()
    val msg = handleRequest(uri)
    val res = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8)
    )
    res.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
    ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
  }

  def handleRequest(uri: String): String = {
    uri match {
      case "/workerInfo" =>
        master.getWorkerInfos()
      case "/threadDump" =>
        master.getThreadDump()
      case _ => s"Unknown uri ${uri}!"
    }
  }
}
