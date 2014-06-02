package com.cba.omnia.piped

import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scalding.typed.IterablePipe

import scalaz._
import scalaz.scalacheck.ScalazArbitrary._

import org.specs2.matcher.Parameters
import org.scalacheck._

class RichErrorPipeSpec extends PipedSpec { def is = s2"""

RichErrorPipe
=============

  handleErrors writes out errors to a separate file and returns the correct data $handleErrors
  handleErrors writes out errors to a separate file and throws an exception on error to terminate the job $handleErrorsException

"""

  implicit val params = Parameters(minTestsOk = 10, maxSize = 10)

  // Work around issue with iterable source where it doesn't seem to like empty string.
  implicit val safeStringArbitrary = Arbitrary.arbitrary[(String, Char)].map { case (s, c) => (s :+ c) }

  def handleErrors = prop { (input: List[ValidationNel[String, Int]]) =>
    class TestJob(args: Args) extends Job(args) with PipeOps {
      IterablePipe(input, flowDef, mode)
        .handleError("fakeError")
        .write(TypedTsv[Int]("fakeOutput"))
    }

    JobTest(new TestJob(_))
      .sink[Int](TypedTsv[Int]("fakeOutput"))(output => {
        val correctResult = input.flatMap(_.toList)
        // Work around bug in contain(exactly)
        if (correctResult.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(correctResult: _*))
      })
      .sink[String](TypedPsv[String]("fakeError"))(output => {
        val correctResult = input.flatMap(_.swap.toList.flatMap(_.list))
        if (correctResult.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(correctResult: _*))
      })
      .run.finish

    ok
  }

  def handleErrorsException = prop { (input: List[ValidationNel[String, Int]]) =>
    val errors = input.flatMap(_.swap.toList.flatMap(_.list))


    class TestJob(args: Args) extends Job(args) with PipeOps {
      val pipe = IterablePipe(input, flowDef, mode)
        .handleError("fakeError", false)
        .write(TypedTsv[Int]("fakeOutput"))
    }

    val block = (_: Any) => {
      JobTest(new TestJob(_))
      .sink[Int](TypedTsv[Int]("fakeOutput"))(output => {
        // If there are errors we should not get to any output
        if (errors.length != 0) failure
        else {
          val correctResult = input.flatMap(_.toList)
          if (correctResult.length == 0 && output.toList.length == 0) ok
          else output.toList must contain(exactly(correctResult: _*))
        }
      })
      .sink[String](TypedPsv[String]("fakeError"))(output => {

        if (errors.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(errors: _*))
      })
      .run.finish
    }

    if (errors.length > 0) block() must throwA[Exception]
    else { block(); ok }
  }
}
