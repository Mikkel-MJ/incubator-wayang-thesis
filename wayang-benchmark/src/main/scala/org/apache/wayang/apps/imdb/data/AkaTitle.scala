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
  * Represents elements from the IMDB `aka_title` table.
  */
case class AkaTitle(id: Integer,
                    movieId: Integer,
                    title: String,
                    imdbIndex: String,
                    kindId: Integer,
                    productionYear: Integer,
                    phoneticCode: String,
                    episodeOfId: Integer,
                    seasonNr: Integer,
                    episodeNr: Integer,
                    note: String,
                    md5sum: String) extends Serializable

object AkaTitle extends Serializable {

  val fields = IndexedSeq("id", "movie_id", "title", "imdb_index", "kind_id", "production_year", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "note", "md5sum")

  /**
    * Parse a CSV row into a [[AkaTitle]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[AkaTitle]]
    */
  def parseCsv(csv: String): AkaTitle = {
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

    AkaTitle(
      get(fields, 0).toInt,
      get(fields, 1).toInt,
      get(fields, 2).trim,
      optStr(fields, 3),
      get(fields, 4).toInt,
      optInt(fields, 5),
      optStr(fields, 6),
      optInt(fields, 7),
      optInt(fields, 8),
      optInt(fields, 9),
      optStr(fields, 10),
      optStr(fields, 11),
    )
  }

  def toTuple(a: AkaTitle) : (Integer, Integer, String, String, Integer, Integer, String, Integer, Integer, Integer, String, String) = {
      (a.id, a.movieId, a.title, a.imdbIndex, a.kindId, a.productionYear, a.phoneticCode, a.episodeOfId, a.seasonNr, a.episodeNr, a.note, a.md5sum)
  }

  def toArray(a: AkaTitle) : Array[AnyRef] = {
      Array(a.id, a.movieId, a.title, a.imdbIndex, a.kindId, a.productionYear, a.phoneticCode, a.episodeOfId, a.seasonNr, a.episodeNr, a.note, a.md5sum)
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
