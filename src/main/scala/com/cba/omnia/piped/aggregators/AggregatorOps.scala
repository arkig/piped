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

case class AggregatorSyntax[A, B, C](agg: Aggregator[A, B, C]) {
  def liftOption: Aggregator[Option[A], Option[B], Option[C]] = AggregatorOps.liftToOptionAggregator(agg)
}


object AggregatorOps extends AggregatorOps

trait AggregatorOps {
  implicit def AggregatorToAggregatorSyntax[A, B, C](aggregator: Aggregator[A, B, C]) = AggregatorSyntax(aggregator)

  def liftToOptionAggregator[A, B, C](aggregator: Aggregator[A, B, C]): Aggregator[Option[A], Option[B], Option[C]] =
    new Aggregator[Option[A], Option[B], Option[C]] {
      def prepare(v: Option[A]) = v.map(aggregator.prepare)

      def reduce(v1: Option[B], v2: Option[B]) = (v1, v2) match {
        case (Some(sv1), Some(sv2)) => Some(aggregator.reduce(sv1, sv2))
        case (Some(sv1), None)      => Some(sv1)
        case (None, Some(sv2))      => Some(sv2)
        case (None, None)           => None
      }

      def present(v: Option[B]) = v.map(aggregator.present)
    }
}
