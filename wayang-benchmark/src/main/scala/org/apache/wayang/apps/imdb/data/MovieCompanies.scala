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
  * Represents elements from the IMDB `movie_companies` table.
  */
case class MovieCompanies(
    id: Integer,
    movieId: Integer,
    companyId: Integer,
    companyTypeId: Integer,
    note: Optional[String]
) extends Serializable

object MovieCompanies extends Serializable {

  val fields = IndexedSeq("id", "movie_id", "company_id", "company_type_id", "note")

  /**
    * Parse a CSV row into a [[MovieCompanies]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[MovieCompanies]]
    */
  def parseCsv(csv: String): MovieCompanies = {
    val csvFormat = CSVFormat.DEFAULT.withQuote('"').builder()
        .build();
    val fields = csvFormat.parse(new StringReader(csv)).getRecords().get(0).toList.asScala;

    assert(1 == 2, "movie_companies " + fields.mkString)

    MovieCompanies(
      fields(0).toInt,
      fields(1).toInt,
      fields(2).toInt,
      fields(3).toInt,
      if (fields.length > 4) Optional.of(fields(4)) else Optional.empty()
    )

  }

  def toTuple(m: MovieCompanies): (Integer, Integer, Integer, Integer, Optional[String]) = {
    (m.id, m.movieId, m.companyId, m.companyTypeId, m.note)
  }

  def toArray(m: MovieCompanies) : Array[AnyRef] = {
    Array(m.id, m.movieId, m.companyId, m.companyTypeId, m.note)
  }
}

