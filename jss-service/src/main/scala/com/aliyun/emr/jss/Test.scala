package com.aliyun.emr.jss

import com.aliyun.emr.jss.common.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvConfig}
import com.aliyun.emr.jss.common.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.SparkConf

object Test {

  def main(args: Array[String]): Unit = {
    val config = RpcEnvConfig(new SparkConf(), "hello-server", "localhost", "localhost", 52345, 0, false)
    val rpcEnv: RpcEnv = new NettyRpcEnvFactory().create(config)
    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("hello-service", helloEndpoint)
    rpcEnv.awaitTermination()
  }
}

class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      //println(s"receive $msg")
      context.reply(s"$msg")
    }
    case SayBye(msg) => {
      //println(s"receive $msg")
      context.reply(s"bye, $msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }
}


case class SayHi(msg: String)

case class SayBye(msg: String)
