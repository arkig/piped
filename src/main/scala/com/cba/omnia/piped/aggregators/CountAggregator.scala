package com.cba.omnia.piped.aggregators

import com.twitter.algebird._

/**
  *  A simple count aggregator.
  *  It counts every value it sees. This aggregator exists so that it can be composed with other aggregators.
  *  If you are just after a count there are probably better ways than this.
  */
case class CountAggregator[T]() extends Aggregator[T, Long, Long] {
  def prepare(v: T) = 1L
  def reduce(l: Long, r: Long) = l + r
  def present(v: Long) = v
}
