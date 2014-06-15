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
