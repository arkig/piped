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
