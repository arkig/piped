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
