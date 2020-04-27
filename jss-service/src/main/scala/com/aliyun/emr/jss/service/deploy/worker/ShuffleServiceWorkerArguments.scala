package com.aliyun.emr.jss.service.deploy.worker

import com.aliyun.emr.jss.common.util.Utils
import org.apache.spark.SparkConf

class ShuffleServiceWorkerArguments (args: Array[String], conf: SparkConf) {

  var host = Utils.localHostName()
  var port = 0
  var propertiesFile: String = null

  // TODO parse args and read from SparkConf later

}
