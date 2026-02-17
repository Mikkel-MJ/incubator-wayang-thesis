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
import scala.collection.mutable.Buffer

/**
  * Represents elements from the IMDB `name` table.
  */
case class Name(id: Integer,
                name: String,
                imdbIndex: String,
                imdbId: Integer,
                gender: String,
                namePcodeCf: String,
                namePcodeNf: String,
                surnamePcode: String,
                md5sum: String) extends Serializable

object Name extends Serializable {

  val fields = IndexedSeq("id", "name", "imdb_index", "imdb_id", "gender", "name_pcode_cf", "name_pcode_nf", "surname_pcode", "md5sum")

  /**
    * Parse a CSV row into a [[Name]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Name]]
    */
  def parseCsv(csv: String): Name = {
    val csvFormat = CSVFormat.DEFAULT
        .withQuote('"')
        .withEscape('\\')
        .withIgnoreSurroundingSpaces(true)
        .withIgnoreEmptyLines(false)
        .withNullString("")   // treat empty fields as null
        .builder()
        .build();
    val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

    //val fields = csv.split(',').map(_.trim)

    Name(
      get(fields, 0).toInt,
      get(fields, 1),
      optStr(fields, 2),
      optInt(fields, 3),
      optStr(fields, 4),
      optStr(fields, 5),
      optStr(fields, 7),
      optStr(fields, 8),
      optStr(fields, 9),
    )
  }

  def toTuple(a: Name) : (Integer, String, String, Integer, String, String, String, String, String) = {
      (a.id, a.name, a.imdbIndex, a.imdbId, a.gender, a.namePcodeCf, a.namePcodeNf, a.surnamePcode, a.md5sum)
  }

  def toArray(a: Name) : Array[AnyRef] = {
      Array(a.id, a.name, a.imdbIndex, a.imdbId, a.gender, a.namePcodeCf, a.namePcodeNf, a.surnamePcode, a.md5sum)
  }

  def get(fields: Buffer[String], i: Int): String =
  fields.lift(i).getOrElse("")

  def optStr(fields: Buffer[String], i: Int): String =
    fields.lift(i) match {
      case Some(s) if s != null && s.nonEmpty => s
      case _                                 => null
  }

  def optInt(fields: Buffer[String], i: Int): Integer =
  fields.lift(i) match {
    case Some(s) if s != null && s.nonEmpty => Integer.valueOf(s)
    case _                                 => null
  }

}
