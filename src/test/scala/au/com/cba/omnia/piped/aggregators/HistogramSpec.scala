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

package au.com.cba.omnia.piped.aggregators

import com.twitter.algebird._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

import com.twitter.algebird.BaseProperties

import au.com.cba.omnia.piped.PipedSpec


class HistogramSpec extends PipedSpec { def is = s2"""
  Can create a histogram from a value                      $create
  Histograms follow the monoid laws                        $monoid
  Can use Histogram aggregators                            $aggregator
  """

  def create = {
    (new HistogramMonoid[Int]).create(1) === Histogram[Int](Map(1 -> 1l))
    (new HistogramMonoid[Double]).create(1.0) === Histogram[Double](Map(1.0 -> 1l))
    (new HistogramMonoid[String]).create("test") === Histogram[String](Map("test" -> 1l))
  }

  def monoid = {
    pending("Algebird scalacheck version is incompatiable with specs2 scalacheck version")
    /*implicit val histogramgen = Arbitrary { for {
      map <- arbitrary[Map[Int, Long]]
    } yield Histogram(map) }

    BaseProperties.monoidLaws[Histogram[Int]]*/
  }

  def aggregator = {
    val aggregator = HistogramAggregator.apply[Int]
    val data = List(1, 2, 1, 3, 1, 4, 2)

    aggregator(data) === Histogram(Map((2, 2l), (1, 3l), (3, 1l), (4, 1l)))
  }
}
