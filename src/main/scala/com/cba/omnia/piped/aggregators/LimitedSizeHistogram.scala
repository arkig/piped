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

package com.cba.omnia.piped.aggregators

import com.twitter.algebird._

import scalaz.{Monoid => _, _}
import scalaz.Scalaz._

/**
  * A losey histogram that only keeps track of less than limit number
  * of items. As soon as the limit is reached the counts are no longer
  * accurately recorded.
  */

sealed trait LimitedSizeHistogram[V] {
  def prettyFormat(itemSeperator: String = ",", countSeperator: String = ":") = this match {
    case OverLimitHistogram()  => "LIMIT EXCEEDED"
    case LimitedHistogram(map) => map.toList.sortBy(_._2).map { case (key, count) => s"$key$countSeperator$count" }.mkString(itemSeperator)
  }

  def prettyFormatWithCount(itemSeperator: String = ",", countSeperator: String = ":", sizeSeperator: String = ";"): String = {
    val hs = prettyFormat(itemSeperator, countSeperator)

    this match {
      case OverLimitHistogram()  => s"$hs$sizeSeperator$hs"
      case LimitedHistogram(map) => s"${map.size}$sizeSeperator$hs"
    }
  }
}

case class LimitedHistogram[V](histogram: Map[V, Long]) extends LimitedSizeHistogram[V] {
  def apply(v: V) = histogram.get(v)
  def add(v: V) = LimitedHistogram(histogram + (v -> histogram.get(v).getOrElse(0l)))
}

case class OverLimitHistogram[V]() extends LimitedSizeHistogram[V]

case class LimitedSizeHistogramMonoid[V](limit: Int) extends Monoid[LimitedSizeHistogram[V]] {
  val zero: LimitedSizeHistogram[V] = LimitedHistogram(Map.empty[V, Long])

  def create(v: V): LimitedSizeHistogram[V] = LimitedHistogram(Map(v -> 1l))

  def create(v: Seq[V]): LimitedSizeHistogram[V] =
    if (v.size < limit) LimitedHistogram(v.map((_ -> 1l)).toMap)
    else OverLimitHistogram[V]

  def create(map: Map[V, Long]): LimitedSizeHistogram[V] =
    if (map.size < limit) LimitedHistogram(map)
    else OverLimitHistogram[V]

  def create(histogram: Histogram[V]): LimitedSizeHistogram[V] =
    if (histogram.histogram.size < limit) LimitedHistogram(histogram.histogram)
    else OverLimitHistogram[V]

  def plus(lsh1: LimitedSizeHistogram[V], lsh2: LimitedSizeHistogram[V]): LimitedSizeHistogram[V] = (lsh1, lsh2) match {
    case (OverLimitHistogram(), _) => lsh1
    case (_, OverLimitHistogram()) => lsh2
    case (LimitedHistogram(h1), LimitedHistogram(h2)) => {
      val combined = h1 |+| h2
      if (combined.size >= limit) OverLimitHistogram[V]
      else LimitedHistogram(combined)
    }
  }
}

case class LimitedSizeHistogramAggregator[V](monoid: LimitedSizeHistogramMonoid[V]) extends MonoidAggregator[V, LimitedSizeHistogram[V], LimitedSizeHistogram[V]] {
  def prepare(value: V) = monoid.create(value)
  def present(h: LimitedSizeHistogram[V]) = h
}

object LimitedSizeHistogramAggregator {
  def apply[V](limit: Int): LimitedSizeHistogramAggregator[V] =
    LimitedSizeHistogramAggregator[V](new LimitedSizeHistogramMonoid[V](limit))
}
