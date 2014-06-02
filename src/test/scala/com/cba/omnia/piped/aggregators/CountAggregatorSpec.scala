package com.cba.omnia.piped
package aggregators

import org.specs2.matcher.Parameters

class CountAggregatorSpec extends PipedSpec { def is = s2"""
  Count Aggregator
  ================

  Can count a list of values $count

  """

  // TODO(hoermast) Find out why this doesn't work
  implicit val params = Parameters(minSize = 1)

  def count = prop { (data: List[String], x: String) =>
    CountAggregator[Any]()(x +: data) === (data.length + 1)
  }
}
