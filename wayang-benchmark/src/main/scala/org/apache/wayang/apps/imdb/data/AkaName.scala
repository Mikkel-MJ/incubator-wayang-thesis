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

import java.util.Optional;
import scala.util.matching.Regex
import org.apache.commons.lang3.StringEscapeUtils

/**
  * Represents elements from the IMDB `aka_name` table.
  */
case class AkaName(id: Integer,
                    personId: Integer,
                    name: String,
                    imdbIndex: String,
                    namePcodeCf: String,
                    namePcodeNf: String,
                    surnamePcode: String,
                    md5sum: String) extends Serializable

object AkaName extends Serializable {

  val fields = IndexedSeq("id", "person_id", "name", "imdb_index", "name_pcode_cf", "name_pcode_nf", "surname_pcode", "md5sum")

  /**
    * Parse a CSV row into a [[AkaName]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[AkaName]]
    */
  def parseCsv(csv: String): AkaName = {
    val pattern: Regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)".r
    val fields = pattern.split(s"""$csv""").map(_.trim)

    //val fields = csv.split(',').map(_.trim)

    AkaName(
      fields(0).toInt,
      fields(1).toInt,
      fields(2).trim,
      fields(3),
      fields(4),
      fields(5),
      fields(6),
      fields(7)
    )
  }

  def toTuple(a: AkaName) : (Integer, Integer, String, String, String, String, String, String) = {
      (a.id, a.personId, a.name, a.imdbIndex, a.namePcodeCf, a.namePcodeNf, a.surnamePcode, a.md5sum)
  }

  def toArray(a: AkaName) : Array[AnyRef] = {
    Array(a.id, a.personId, a.name, a.imdbIndex, a.namePcodeCf, a.namePcodeNf, a.surnamePcode, a.md5sum)
  }

}
