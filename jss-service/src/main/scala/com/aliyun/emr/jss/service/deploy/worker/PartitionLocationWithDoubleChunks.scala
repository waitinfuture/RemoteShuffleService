package com.aliyun.emr.jss.service.deploy.worker

import com.aliyun.emr.jss.protocol.PartitionLocation

class PartitionLocationWithDoubleChunks(
  val location: PartitionLocation,
  val doubleChunk: DoubleChunk
) extends PartitionLocation(location)
