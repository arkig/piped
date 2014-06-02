package com.cba.omnia.piped

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
