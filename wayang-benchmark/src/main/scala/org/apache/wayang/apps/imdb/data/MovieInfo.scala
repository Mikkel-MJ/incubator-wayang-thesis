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
  * Represents elements from the IMDB `movie_info` table.
  */
case class MovieInfo(
    id: Integer,
    movieId: Integer,
    infoTypeId: Integer,
    info: String,
    note: String
) extends Serializable

object MovieInfo extends Serializable {

  val fields = IndexedSeq("id", "movie_id", "info_type_id", "info", "note")

  /**
    * Parse a CSV row into a [[MovieInfo]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[MovieInfo]]
    */
  def parseCsv(csv: String): MovieInfo = {
    val csvFormat = CSVFormat.DEFAULT
        .withQuote('"')
        .withEscape('\\')
        .withIgnoreSurroundingSpaces(true)
        .builder()
        .build();
    val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;


    MovieInfo(
      fields(0).toInt,
      fields(1).toInt,
      fields(2).toInt,
      fields(3),
      if (fields.length > 4 && fields(4).nonEmpty) fields(4) else null
    )
  }

  def toTuple(m: MovieInfo): (Integer, Integer, Integer, String, String) = {
    (m.id, m.movieId, m.infoTypeId, m.info, m.note)
  }

  def toArray(m: MovieInfo) : Array[AnyRef] = {
    Array(m.id, m.movieId, m.infoTypeId, m.info, m.note)
  }
}

