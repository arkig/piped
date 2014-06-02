package com.cba.omnia.piped.utils

import scala.sys.process._

import java.io.File
import java.util.UUID

import au.com.cba.omnia.omnitool.Closeable

class NamedPipe(val file: File) {
  val path = file.getAbsolutePath
  def delete = file.delete
}

object NamedPipe {
  implicit val NamedPipeToCloseable = Closeable[NamedPipe](_.delete)

  def createTempPipe(prefix: String = "", suffix: String = ".pipe", directory: File = new File(System.getProperty("java.io.tmpdir"))): NamedPipe = {
    val p = NamedPipe(new File(directory, s"${prefix}${UUID.randomUUID}$suffix"))
    val output = new StringBuilder
    val errors = new StringBuilder
    val logger = ProcessLogger(out => output append out, err => errors append err)
    val ret = s"mkfifo ${p.path}" ! logger

    if (ret == 0) p
    else throw new Exception(s"Failed to create fifo: ${output.mkString}\n${errors.mkString}")
  }

  def apply(path: String): NamedPipe = new NamedPipe(new File(path))
  def apply(file: File): NamedPipe = new NamedPipe(file)
}
