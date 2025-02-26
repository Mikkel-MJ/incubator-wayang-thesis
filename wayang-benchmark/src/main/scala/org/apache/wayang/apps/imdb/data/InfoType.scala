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
  * Represents elements from the IMDB `info_type` table.
  */
case class InfoType(id: Integer, info: String) extends Serializable

object InfoType extends Serializable {

  val fields = IndexedSeq("id", "info")

  /**
    * Parse a CSV row into an [[InfoType]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[InfoType]]
    */
  def parseCsv(csv: String): InfoType = {
    val csvFormat = CSVFormat.DEFAULT.withQuote('"').builder()
        .build();
    val fields = csvFormat.parse(new StringReader(csv)).getRecords().get(0).toList.asScala;

    InfoType(
      fields(0).toInt,
      fields(1)
    )
  }

  def toTuple(i: InfoType): (Integer, String) = {
    (i.id, i.info)
  }

  def toArray(i: InfoType) : Array[AnyRef] = {
    Array(i.id, i.info)
  }
}

