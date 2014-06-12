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

package com.cba.omnia.piped

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.omnitool.Validated

import PipeOps._

import org.specs2.matcher.Parameters

class RichGroupedSpec extends PipedSpec { def is = s2"""

RichGrouped
=============

  map a partial value across the values $mapValuesPartialGrouped

RichKeyedList
=============

  map a partial value across the values $mapValuesPartial
  run values through R on the reducer side $processValuesWithR
  run values through R on the reducer side and produce meaningful error messages $processValuesWithRError
  run tuple values through R as CSV on the reducer side $processValuesWithRCSV

"""

  implicit val params = Parameters(minTestsOk = 10, maxSize = 10)

  val rCommand1 = """x <- scan(input); y <- x + 1; write(y, output, sep="\n")"""
  val rCommand2 = """3 / "a""""
  val rCommand3 = """x <- read.csv(input,header=FALSE); x[1] <- x[1] + 1; x[2] <- x[2] - 1; write.table(x, output, row.names=FALSE,col.names=FALSE,sep=",")"""

  def mapValuesPartialGrouped = prop { (input: List[(Int, Int)]) =>
    class TestJob(args: Args) extends Job(args) with GroupedOps {
      IterablePipe(input, flowDef, mode)
        .group
        .mapValuesPartialGrouped(_ => false) { case x if x % 3 == 0 => true }
        .toTypedPipe
        .write(TypedTsv[(Int, Boolean)]("fakeOutput"))
    }

    JobTest(new TestJob(_))
      .sink[(Int, Boolean)](TypedTsv[(Int, Boolean)]("fakeOutput"))(output => {
        val correctResult = input.map(x => (x._1, x._2 % 3 == 0))
        // Work around bug in contain(exactly)
        if (correctResult.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(correctResult: _*))
      })
      .run.finish
  }

  def processValuesWithR = prop { (data: List[(Int, Int)]) =>
    val input = data.map { case (k, v) => (k % 2, v  % 1000) }

    class TestJob(args: Args) extends Job(args) with GroupedOps {
      IterablePipe(input, flowDef, mode)
        .group
        .mapValues(_.toString)
        .processValuesWithR(rCommand1, _.map(x => Validated.safe(x.toInt)))
        .toTypedPipe
        .map { case (k, v) => v.map((k, _)) }
        .handleError("fakeErrors", false)
        .write(TypedTsv[(Int, Int)]("fakeOutput"))
    }

    JobTest(new TestJob(_))
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("fakeOutput"))(output => {
        val correctResult = input.map { case (k, v) =>  (k, v + 1) }
        if (correctResult.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(correctResult: _*))
      })
      .sink[String](TypedPsv[String]("fakeErrors"))(_ must be empty)
      .run
      .finish

  }

  def processValuesWithRError = {
    class TestJob(args: Args) extends Job(args) with GroupedOps {
      IterablePipe(List((1,2)), flowDef, mode)
        .group
        .mapValues(_.toString)
        .processValuesWithR(rCommand2, _.map(x => Validated.safe(x.toInt)))
        .toTypedPipe
        .map { case (k, v) => v.map((k, _)) }
        .handleError("fakeErrors", true)
        .write(TypedTsv[(Int, Int)]("fakeOutput"))
    }

    JobTest(new TestJob(_))
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("fakeOutput"))(_ must be empty)
      .sink[String](TypedPsv[String]("fakeErrors"))(output => {
        output.toList.mkString("; ").trim ===
          """Failed to run R command: 3 / "a"; Error in 3/"a" : non-numeric argument to binary operator"""
      })
      .run
      .finish

    ok
  }

  def processValuesWithRCSV = prop { (data: List[(Int, Int, Int)]) =>
    val input = data.map { case (k, v1, v2) => (k % 2, (v1  % 1000, v2 % 1000)) }

    def parse(s: String) = Validated.safe {
      val x = s.split(",")
      (x(0).toInt, x(1).toInt)
    }

    class TestJob(args: Args) extends Job(args) with GroupedOps {
      IterablePipe(input, flowDef, mode)
        .group
        .processValuesWithRCSV(rCommand3, _.map(parse))
        .toTypedPipe
        .map { case (k, v) => v.map(x => (k, x._1, x._2)) }
        .handleError("fakeErrors", false)
        .write(TypedTsv[(Int, Int, Int)]("fakeOutput"))
    }

    JobTest(new TestJob(_))
      .sink[(Int, Int, Int)](TypedTsv[(Int, Int, Int)]("fakeOutput"))(output => {
        val correctResult = input.map { case (k, (v1, v2)) =>  (k, v1 + 1, v2 - 1) }
        if (correctResult.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(correctResult: _*))
      })
      .sink[String](TypedPsv[String]("fakeErrors"))(_ must be empty)
      .run
      .finish

  }

  def mapValuesPartial = prop { (input: List[(Int, Int)]) =>
    class TestJob(args: Args) extends Job(args) with GroupedOps {
      IterablePipe(input, flowDef, mode)
        .group
        .mapValuesPartial(_ => false) { case x if x % 3 == 0 => true }
        .toTypedPipe
        .write(TypedTsv[(Int, Boolean)]("fakeOutput"))
    }

    JobTest(new TestJob(_))
      .sink[(Int, Boolean)](TypedTsv[(Int, Boolean)]("fakeOutput"))(output => {
        val correctResult = input.map(x => (x._1, x._2 % 3 == 0))
        // Work around bug in contain(exactly)
        if (correctResult.length == 0 && output.toList.length == 0) ok
        else output.toList must contain(exactly(correctResult: _*))
      })
      .run.finish
  }
}
