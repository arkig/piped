//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.piped.utils

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
