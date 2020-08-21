package com.aliyun.emr.ess.service.deploy.worker.http

import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.sink.PrometheusHttpRequestHandler
import com.aliyun.emr.ess.service.deploy.master.Master
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil

@Sharable
class HttpRequestHandler(prometheusHttpRequestHandler: PrometheusHttpRequestHandler)
  extends SimpleChannelInboundHandler[FullHttpRequest] with Logging{

  private val INVALID = "invalid"

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.uri()
    val msg = prometheusHttpRequestHandler.handleRequest(uri)

    val res = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8)
    )
    res.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
    ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
  }
}
