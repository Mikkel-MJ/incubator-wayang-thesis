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
import org.apache.commons.csv._
import java.io.StringReader;
import scala.util.matching.Regex
import scala.collection.JavaConverters._

/**
  * Represents elements from the IMDB `person_info` table.
  */
case class PersonInfo(
    id: Integer,
    personId: Integer,
    infoTypeId: Integer,
    info: String,
    note: String
) extends Serializable

object PersonInfo extends Serializable {

  val fields = IndexedSeq("id", "person_id", "info_type_id", "info", "note")

  /**
    * Parse a CSV row into a [[PersonInfo]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[PersonInfo]]
    */
  def parseCsv(csv: String): PersonInfo = {
    val csvFormat = CSVFormat.DEFAULT
        .withQuote('"')
        .withEscape('\\')
        .withIgnoreSurroundingSpaces(true)
        .builder()
        .build();
    val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

    PersonInfo(
      fields(0).toInt,
      fields(1).toInt,
      fields(2).toInt,
      fields(3),
      if (fields.length > 4 && fields(4).nonEmpty) fields(4) else null
    )
  }

  def toTuple(p: PersonInfo): (Integer, Integer, Integer, String, String) = {
    (p.id, p.personId, p.infoTypeId, p.info, p.note)
  }

  def toArray(p: PersonInfo) : Array[AnyRef] = {
    Array(p.id, p.personId, p.infoTypeId, p.info, p.note)
  }
}

