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

import scala.concurrent._
import scala.concurrent.duration.{Duration, MINUTES}
import scala.collection.mutable.ListBuffer

import java.util.concurrent.Executors
import java.io.{BufferedReader, FileReader, PrintWriter}

import com.twitter.scalding.{Duration => _, _}, TDsl._
import com.twitter.scalding.typed.{Grouped, UnsortedGrouped, KeyedListLike}

import scalaz._, Scalaz._

import org.rosuda.REngine.Rserve.RConnection

import au.com.cba.omnia.omnitool._, Closeable._

import utils.NamedPipe

object GroupedOps extends GroupedOps

trait GroupedOps {
  implicit def GroupedToRichGrouped[A, B](grouped: Grouped[A, B]) = RichGrouped(grouped)
  implicit def KeyedListToRichKeyedList[A, B, This[A, +B] <: KeyedListLike[A, B, This]](keyedList: KeyedListLike[A, B, This]) = RichKeyedListLike(keyedList)
}

case class RichGrouped[A, B](grouped: Grouped[A, B]) extends GroupedOps {
  def mapValuesPartialGrouped[C](alternative: B => C)(f: PartialFunction[B, C]): UnsortedGrouped[A, C] =
    grouped.mapValues(v => f.lift(v).getOrElse(alternative(v)))

}

case class RichKeyedListLike[A, +B, +This[A, +B] <: KeyedListLike[A, B, This]](keyedList: KeyedListLike[A, B, This]) extends GroupedOps {
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))

  def mapValuesPartial[C](alternative: B => C)(f: PartialFunction[B, C]): This[A, C] =
    keyedList.mapValues(v => f.lift(v).getOrElse(alternative(v)))

  /**
    * Runs the values through R for each group of values for a specific key.
    * The R command can read in the values line by line from a fifo referenced by the variable ```input```.
    * Output needs to be written to a fifo referenced by the variable ```output```.
    * The RServe library needs to be installed and running on each node.
    *
    * In R: {{{
        > library(Rserve)
        > Rserve()
      }}}
    *
    * A sample invocation: {{{
       val command = """x <- scan(input); y <- x + 1; write(y, output, sep="\n")"""
       processValuesWithR(command,_.map(x => Validated.safe(x.toInt)))
      }}}
    * @param command the R command to execute. Needs to read date from ```input``` and write it to ```output```.
    * @param processROutput takes an Iterator over R output lines and returns an Iterator of the result.
    * @param connect a function that creates an RConnection
    */
  def processValuesWithR[U]
    (command: String, processROutput: Iterator[String] => Iterator[Validated[U]], connect: () => RConnection = () => new RConnection())
    (implicit ev: B<:<String): This[A, Validated[U]] = {

    implicit val RConnectionToCloseable = Closeable[RConnection](_.close)

    keyedList.mapValueStream(data => {
      val results: Validated[Iterator[String]] = Validated.safe {
        NamedPipe.createTempPipe("r_input").doAndRelease { inputPipe =>
          connect().doAndRelease { connection =>
            Future {
              new PrintWriter(inputPipe.file).doAndRelease { inputWriter =>
                data.foreach(inputWriter.println)
              }
            }

            connection.eval(s"""input <- fifo("${inputPipe.path}", "r", TRUE)""")
            connection.assign(".tmp_code.", command)

            NamedPipe.createTempPipe("r_output").doAndRelease { outputPipe =>
              val read = Future {
                new BufferedReader(new FileReader(outputPipe.file)).doAndRelease { outputReader =>
                  val results = ListBuffer.empty[String]
                  var line = outputReader.readLine
                  while (line != null) {
                    results += line
                    line = outputReader.readLine
                  }

                  results.iterator.right
                }
              }

              connection.eval(s"""output <- fifo("${outputPipe.path}", "w", TRUE)""")
              val result = connection.parseAndEval("try(eval(parse(text=.tmp_code.)),silent=TRUE)")

              connection.eval("close(output)")
              if (result.inherits("try-error")) NonEmptyList(result.asString).left[Iterator[String]]
              else Await.result(read, Duration(10, MINUTES))
            }
          }
        }
      }.disjunction.join.validation.addError("Failed to run R command: " + command)

      results match {
        case Success(iter)  => processROutput(iter)
        case Failure(error) => Iterator(error.failure)
      }
    })
  }

  /**
    * Runs the values through R for each group of values for a specific key.
    * The R command can read in the values line by line from a fifo referenced by the variable ```input```.
    * The input is in CSV format.
    * Output needs to be written to a fifo referenced by the variable ```output```.
    * The RServe library needs to be installed and running on each node.
    *
    * In R: {{{
    > library(Rserve)
    > Rserve()
    }}}
    *
    * A sample invocation: {{{
    val command = """x <- read.csv(input,header=FALSE); x[1] <- x[1] + 1; x[2] <- x[2] - 1; write.table(x, output, row.names=FALSE,col.names=FALSE,sep=",")"""
    processValuesWithRCSV(command,_.map(x => Validated.safe(x.toInt)))
    }}}
    * @param command the R command to execute. Needs to read date from ```input``` and write it to ```output```.
    * @param processROutput takes an Iterator over R output lines and returns an Iterator of the result.
    * @param connect a function that creates an RConnection
    */

  def processValuesWithRCSV[U]
    (command: String, processROutput: Iterator[String] => Iterator[Validated[U]], connect: () => RConnection = () => new RConnection())
    (implicit ev: B <:< Product): This[A, Validated[U]] = {
    keyedList
      .mapValues(_.productIterator.toList.mkString(","))
      .processValuesWithR(command, processROutput, connect)
  }
}
