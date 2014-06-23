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

import scalaz.{Monoid => _}
import scalaz.Scalaz._

case class Histogram[V](histogram: Map[V, Long]) {
  def apply(v: V) = histogram.get(v)
  def add(v: V) = Histogram(histogram + (v -> histogram.get(v).getOrElse(0l)))

  def prettyFormat(itemSeperator: String = ",", countSeperator: String = ":") =
    histogram.toList.sortBy(_._2).map { case (key, count) => s"$key$countSeperator$count" }.mkString(itemSeperator)

  def prettyFormatWithSize(itemSeperator: String = ",", countSeperator: String = ":", sizeSeperator: String = ";") = {
    val hs = prettyFormat(itemSeperator, countSeperator)
    s"${histogram.size}$sizeSeperator$hs"
  }
}

object Histogram {
  implicit def monoid[V] = new HistogramMonoid[V]
  implicit def aggregator[V] = new HistogramAggregator[V]
}

class HistogramMonoid[V] extends Monoid[Histogram[V]] {
  val zero = Histogram(Map.empty[V, Long])

  def create(v: V) = Histogram(Map(v -> 1l))

  def plus(h1: Histogram[V], h2: Histogram[V]) = Histogram(h1.histogram |+| h2.histogram)
}

case class HistogramAggregator[V]() extends MonoidAggregator[V, Histogram[V], Histogram[V]] {
  def monoid = new HistogramMonoid[V]
  def prepare(value: V) = monoid.create(value)
  def present(h: Histogram[V]) = h
}
