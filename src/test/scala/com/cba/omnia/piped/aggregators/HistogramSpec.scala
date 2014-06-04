package com.cba.omnia.piped.aggregators

import com.twitter.algebird._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

import com.cba.omnia.piped.PipedSpec

import com.twitter.algebird.BaseProperties

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
