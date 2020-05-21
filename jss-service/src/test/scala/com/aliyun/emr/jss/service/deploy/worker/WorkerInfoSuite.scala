package com.aliyun.emr.jss.service.deploy.worker

import java.util

import com.aliyun.emr.jss.service.deploy.master.WorkerInfo
import org.scalatest.FunSuite

class WorkerInfoSuite extends FunSuite {
  test("equals") {
    val workerInfo1 = new WorkerInfo("a", "localhost", 8081, 1024, null)
    val workerInfo2 = new WorkerInfo("b", "localhost", 8081, 1022, null)
    assert(workerInfo1.equals(workerInfo2))
    assert(workerInfo1 == workerInfo2)

    val map = new util.HashMap[WorkerInfo, Int]()
    map.put(workerInfo1, 1)
    assert(map.get(workerInfo2) == 1)
    map.put(workerInfo2, 2)
    assert(map.size() == 1)
    assert(map.get(workerInfo1) == 2)
    assert(map.get(workerInfo2) == 2)
  }
}
