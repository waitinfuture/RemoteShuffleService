package com.aliyun.emr.jss

import com.aliyun.emr.jss.common.rpc.netty.NettyRpcEnvFactory
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvConfig}
import org.apache.spark.SparkConf

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Client {

  def main(args: Array[String]): Unit = {
    //    asyncCall()
    syncCall()
  }

  def asyncCall() = {
    val config = RpcEnvConfig(new SparkConf(), "hello-client", "localhost", "localhost", 52345, 0, true)
    val rpcEnv: RpcEnv = new NettyRpcEnvFactory().create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val future: Future[String] = endPointRef.ask[String](SayHi("neo"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def syncCall() = {
    val config = RpcEnvConfig(new SparkConf(), "hello-client", "localhost", "localhost", 52345, 0, true)
    val rpcEnv: RpcEnv = new NettyRpcEnvFactory().create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val result = endPointRef.askSync[String](SayBye("neo"))
    println(result)
  }
}
