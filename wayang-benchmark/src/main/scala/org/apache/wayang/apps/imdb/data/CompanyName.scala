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
  * Represents elements from the IMDB `company_name` table.
  */
case class CompanyName(
    id: Integer,
    name: String,
    countryCode: Option[String],
    imdbId: Option[Integer],
    namePcodeNf: Option[String],
    namePcodeSf: Option[String],
    md5sum: Option[String]
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
    val fields = csv.split(',').map(_.trim)

    CompanyName(
      fields(0).toInt,
      fields(1),
      Option(fields(2)).filter(_.nonEmpty),
      Option(fields(3)).filter(_.nonEmpty).map(_.toInt),
      Option(fields(4)).filter(_.nonEmpty),
      Option(fields(5)).filter(_.nonEmpty),
      Option(fields(6)).filter(_.nonEmpty)
    )
  }

  def toTuple(c: CompanyName): (Integer, String, Option[String], Option[Integer], Option[String], Option[String], Option[String]) = {
    (c.id, c.name, c.countryCode, c.imdbId, c.namePcodeNf, c.namePcodeSf, c.md5sum)
  }

  def toArray(c: CompanyName) : Array[AnyRef] = {
    Array(c.id, c.name, c.countryCode, c.imdbId, c.namePcodeNf, c.namePcodeSf, c.md5sum)
  }
}

