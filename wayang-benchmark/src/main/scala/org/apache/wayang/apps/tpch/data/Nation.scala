/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.tpch.data

/**
  * Represents elements from the TPC-H `NATION` table.
  */
case class Nation(nationKey: Long,
                    name: String,
                    regionKey: Long,
                    comment: String) extends Serializable

object Nation extends Serializable {

  val fields = IndexedSeq("n_nationkey", "n_name", "n_regionkey","n_comment")

  /**
    * Parse a CSV row into a [[Nation]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Nation]]
    */
  def parseCsv(csv: String): Nation = {
    val fields = csv.split('|').map(_.trim)

    Nation(
      fields(0).toLong,
      fields(1),
      fields(2).toLong,
      fields(3)
    )
  }

  def toTuple(n: Nation): (Long, String, Long, String) = {
    (n.nationKey, n.name, n.regionKey, n.comment)
  }

}
