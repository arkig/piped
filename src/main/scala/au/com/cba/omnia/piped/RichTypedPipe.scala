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

package au.com.cba.omnia.piped

import scalaz.ValidationNel

import com.twitter.scalding._
import com.twitter.scalding.TDsl._

import cascading.flow.FlowDef

object PipeOps extends PipeOps

trait PipeOps {
  implicit def TypedPipeToRichErrorPipe[T](pipe: TypedPipe[ValidationNel[String, T]]) = RichErrorPipe[T](pipe)
}

case class RichErrorPipe[T](pipe: TypedPipe[ValidationNel[String, T]]) {
  /**
    * Writes out any errors to the specified file and if continue is false throws an exception afterwards to terminate the job.
    */
  def handleError(errorPath: String, continue: Boolean = true)(implicit flow: FlowDef, mode: Mode): TypedPipe[T] = {
    // TODO(hoermast) revisit once we have determined the best way to handle failures in scalding.
    pipe.
      flatMap(_.swap.toList.flatMap(_.list))
      .write(TypedPsv[String](errorPath))

    if (continue) pipe.flatMap(_.toOption)
    else pipe.map(_.valueOr(nel => throw new Exception(nel.list.mkString("; "))))
  }
}
