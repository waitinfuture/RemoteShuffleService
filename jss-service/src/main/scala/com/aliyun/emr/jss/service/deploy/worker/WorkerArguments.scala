package com.aliyun.emr.jss.service.deploy.worker

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.util.Utils

class WorkerArguments(args: Array[String], conf: JindoConf) {

  var host = Utils.localHostName()
  var port = 0
  var propertiesFile: String = null

  // TODO parse args and read from SparkConf later

}
