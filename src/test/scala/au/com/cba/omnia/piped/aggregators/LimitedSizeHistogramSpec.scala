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

import au.com.cba.omnia.piped.PipedSpec

import com.twitter.algebird.BaseProperties

class LimitedSizeHistogramSpec extends PipedSpec { def is = s2"""
  Can create a LimitedSizeHistogram from a value                    $create
  LimitedSizeHistograms follow the monoid laws                      $monoid
  Can use LimitedSizeHistogram aggregators                          $aggregator
  LimitedSizeHistograms have a limited size                         $limited
  LimitedSizeHistograms have a limited size when used as aggregator $limitedAggregator
  """

  def create = {
    (LimitedSizeHistogramMonoid[Int](5)).create(1) === LimitedHistogram[Int](Map(1 -> 1l))
    (LimitedSizeHistogramMonoid[Double](5)).create(1.0) === LimitedHistogram[Double](Map(1.0 -> 1l))
    (LimitedSizeHistogramMonoid[String](5)).create("test") === LimitedHistogram[String](Map("test" -> 1l))
  }

  def monoid = {
    pending("Algebird scalacheck version is incompatiable with specs2 scalacheck version")
    /*implicit val monoid = LimitedSizeHistogramMonoid[Int](7)
    implicit val histogramgen: Arbitrary[LimitedSizeHistogram[Int]] = Arbitrary { for {
      map <- arbitrary[Map[Int, Long]]
    } yield monoid.create(map) }

    BaseProperties.monoidLaws[LimitedSizeHistogram[Int]]*/
  }

  def aggregator = {
    val aggregator = LimitedSizeHistogramAggregator.apply[Int](5)
    val data = List(1, 2, 1, 3, 1, 4, 2)

    aggregator(data) === LimitedHistogram(Map((2, 2l), (1, 3l), (3, 1l), (4, 1l)))
  }

  def limited = {
    val monoid = LimitedSizeHistogramMonoid[Int](4)
    val data = List(1, 2, 1, 3, 1, 4, 2, 5, 7)
    data.map(monoid.create).foldLeft(monoid.zero)(monoid.plus) === OverLimitHistogram[Int]()
  }

  def limitedAggregator = {
    val aggregator = LimitedSizeHistogramAggregator.apply[Int](5)
    val data = List(1, 2, 1, 3, 1, 4, 2, 5, 6, 7)

    aggregator(data) === OverLimitHistogram[Int]
  }

}
