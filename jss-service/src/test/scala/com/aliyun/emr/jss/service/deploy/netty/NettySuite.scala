package com.aliyun.emr.jss.service.deploy.netty

import scala.util.Random

import io.netty.buffer.{ByteBuf, Unpooled}
import org.scalatest.FunSuite

class NettySuite extends FunSuite {
  def printData(buf: ByteBuf): Unit = {
    0 until buf.readableBytes() foreach(ind => print(buf.getByte(buf.readerIndex() + ind) + " "))
    println
  }

  test("CompositeByteBuf") {
    val data = new Array[Byte](63)
    Random.nextBytes(data)
    val buf1 = Unpooled.copiedBuffer(data)
    val buf2 = Unpooled.copiedBuffer(data)
    val frame = buf1.alloc().compositeBuffer(Integer.MAX_VALUE)
    println(buf1.readableBytes())
    frame.addComponent(buf1).writerIndex(frame.writerIndex() + buf1.readableBytes())
    frame.addComponent(buf2).writerIndex(frame.writerIndex() + buf2.readableBytes())
    println(frame.readableBytes())
    println(frame.readerIndex())
    println(frame.writerIndex())

    printData(buf1)
    printData(buf2)
    printData(frame)
  }
}
