package com.aliyun.emr.jss.service.deploy.master

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.util.Utils

class MasterArguments(args: Array[String], conf: JindoConf) {

  var host = Utils.localHostName()
  var port = 9099
  var propertiesFile: String = null

  // TODO parse args and read from SparkConf later

}
