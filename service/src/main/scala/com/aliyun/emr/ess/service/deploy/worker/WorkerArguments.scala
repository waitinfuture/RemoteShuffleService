/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.ess.service.deploy.worker

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.util.{IntParam, MemoryParam, Utils}

import scala.annotation.tailrec

class WorkerArguments(args: Array[String], conf: EssConf) {

  var host = Utils.localHostName()
  var port = 0
  // var master: String = null
  // for local testing.
  var master: String = s"ess://$host:9099"
  var propertiesFile: String = null

  parse(args.toList)

  propertiesFile = Utils.loadDefaultEssProperties(conf, propertiesFile)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      master = value
      parse(tail)

    case Nil =>
      if (master == null) {  // No positional argument was given
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
    * Print usage and exit JVM with the given exit code.
    */
  def printUsageAndExit(exitCode: Int) {
    // scalastyle:off println
    System.err.println(
      "Usage: Worker [options] <master>\n" +
        "\n" +
        "Master must be a URL of the form ess://hostname:port\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST     Hostname to listen on\n" +
        "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
        "  --properties-file FILE   Path to a custom ESS properties file.\n" +
        "                           Default is conf/ess-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }
}
