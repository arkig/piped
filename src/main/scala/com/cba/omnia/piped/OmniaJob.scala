package com.cba.omnia.piped

import com.twitter.scalding._

import org.apache.hadoop.conf.Configuration

class OmniaJob(args: Args) extends Job(args) {

  override def config: Map[AnyRef,AnyRef] = {
    Option(System.getenv("HADOOP_TOKEN_FILE_LOCATION")) match {
      case None => super.config
      case Some(bin) =>  super.config ++ Map("mapreduce.job.credentials.binary" -> bin)
    }
  }

}
