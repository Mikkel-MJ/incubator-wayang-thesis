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
  * Represents elements from the TPC-H `CUSTOMER` table.
  */
case class Customer(custKey: Long,
                    name: String,
                    address: String,
                    nationKey: Long,
                    phone: String,
                    acctbal: Double,
                    mktSegment: String,
                    comment: String) extends Serializable

object Customer extends Serializable {

  val fields = IndexedSeq("c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment")

  /**
    * Parse a CSV row into a [[Customer]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Customer]]
    */
  def parseCsv(csv: String): Customer = {
    val fields = csv.split('|').map(_.trim)

    Customer(
      fields(0).toLong,
      fields(1),
      fields(2),
      fields(3).toLong,
      fields(4),
      fields(5).toDouble,
      fields(6).trim,
      fields(7)
    )
  }

  def toTuple(c: Customer) : (Long, String, String, Long, String, Double, String, String) = {
      (c.custKey, c.name, c.address, c.nationKey, c.phone, c.acctbal, c.mktSegment, c.comment)
  }

}
