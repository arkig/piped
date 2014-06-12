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
