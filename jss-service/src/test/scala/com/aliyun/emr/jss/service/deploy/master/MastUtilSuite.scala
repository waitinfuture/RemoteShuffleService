package com.aliyun.emr.jss.service.deploy.master

import org.scalatest.FunSuite

class MastUtilSuite extends FunSuite{
  test("hello world") {
    assert(1 == 1);
  }

  test("synchronize") {
    def foo(): Unit = {
      println("start")
      Thread.sleep(1000)
      println("end")
    }

    val lock = new Object()
    val threads = 0 until 10 map (_ => {
      new Thread() {
        override def run(): Unit = {
          lock.synchronized {
            foo();
          }
        }
      }
    })
    threads.foreach(_.start())
    threads.foreach(_.join())
  }
}
