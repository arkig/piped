package com.cba.omnia.piped
package aggregators

import com.twitter.algebird._

import com.cba.omnia.piped.aggregators.AggregatorOps._

import com.cba.omnia.piped.PipedSpec

class OptionAggregatorSpec extends PipedSpec { def is = s2"""
Option Aggregator
=================
  Can lift an aggregators into an option aggregator $optionAggregator
  """

  def optionAggregator = {
    val data1 = List(Some(4), Some(1), None, Some(2))
    val data2 = List(None, None, None)
    val MinAgg = new Aggregator[Int, Int, Int] {
      def prepare(v: Int) = v
      def reduce(v1: Int, v2: Int) = Math.min(v1, v2)
      def present(v: Int) = v
    }

    val optionMinAgg = MinAgg.liftOption
    optionMinAgg.reduce(data1) === Some(1)
    optionMinAgg.reduce(data2) === None
  }
}
