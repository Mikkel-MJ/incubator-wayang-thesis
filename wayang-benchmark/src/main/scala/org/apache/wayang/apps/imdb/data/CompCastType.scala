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

package org.apache.wayang.apps.imdb.data

/**
  * Represents elements from the IMDB `comp_cast_type` table.
  */
case class CompCastType(id: Integer, kind: String) extends Serializable

object CompCastType extends Serializable {

  val fields = IndexedSeq("id", "kind")

  /**
    * Parse a CSV row into a [[CompCastType]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[CompCastType]]
    */
  def parseCsv(csv: String): CompCastType = {
    val fields = csv.split(',').map(_.trim)

    CompCastType(
      fields(0).toInt,
      fields(1)
    )
  }

  def toTuple(c: CompCastType): (Integer, String) = {
    (c.id, c.kind)
  }

  def toArray(c: CompCastType) : Array[AnyRef] = {
    Array(c.id, c.kind)
  }
}

