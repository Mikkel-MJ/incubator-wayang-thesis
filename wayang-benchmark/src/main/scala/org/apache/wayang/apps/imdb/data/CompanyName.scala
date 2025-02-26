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
  * Represents elements from the IMDB `company_name` table.
  */
case class CompanyName(
    id: Integer,
    name: String,
    countryCode: Optional[String],
    imdbId: Optional[Integer],
    namePcodeNf: Optional[String],
    namePcodeSf: Optional[String],
    md5sum: Optional[String]
) extends Serializable

object CompanyName extends Serializable {

  val fields = IndexedSeq("id", "name", "country_code", "imdb_id", "name_pcode_nf", "name_pcode_sf", "md5sum")

  /**
    * Parse a CSV row into a [[CompanyName]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[CompanyName]]
    */
  def parseCsv(csv: String): CompanyName = {
    val csvFormat = CSVFormat.DEFAULT.withQuote('"').builder()
        .build();
    val fields = csvFormat.parse(new StringReader(csv)).getRecords().get(0).toList.asScala;
    //val fields = csv.split(',').map(_.trim)

    CompanyName(
      fields(0).toInt,
      fields(1),
      if (fields.length > 2) Optional.of(fields(2)).filter(_.nonEmpty) else Optional.empty(),
      if (fields.length > 3 && fields(3).nonEmpty) Optional.of(fields(3).toInt) else Optional.empty(),
      if (fields.length > 4) Optional.of(fields(4)).filter(_.nonEmpty) else Optional.empty(),
      if (fields.length > 5) Optional.of(fields(5)).filter(_.nonEmpty) else Optional.empty(),
      if (fields.length > 6) Optional.of(fields(6)).filter(_.nonEmpty) else Optional.empty(),
    )
  }

  def toTuple(c: CompanyName): (Integer, String, Optional[String], Optional[Integer], Optional[String], Optional[String], Optional[String]) = {
    (c.id, c.name, c.countryCode, c.imdbId, c.namePcodeNf, c.namePcodeSf, c.md5sum)
  }

  def toArray(c: CompanyName) : Array[AnyRef] = {
    Array(c.id, c.name, c.countryCode, c.imdbId, c.namePcodeNf, c.namePcodeSf, c.md5sum)
  }
}

