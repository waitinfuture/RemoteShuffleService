package com.aliyun.emr.jss.service.deploy.master

import com.aliyun.emr.jss.common.util.Utils
import org.apache.spark.SparkConf

class ShuffleServiceMasterArguments(args: Array[String], conf: SparkConf) {

  var host = Utils.localHostName()
  var port = 9099
  var propertiesFile: String = null

  // TODO parse args and read from SparkConf later

}
