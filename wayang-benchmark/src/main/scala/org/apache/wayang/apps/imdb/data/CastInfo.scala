package org.apache.wayang.apps.imdb.data

import java.util.Optional;
import org.apache.commons.csv._
import java.io.StringReader;
import scala.collection.JavaConverters._

/**
  * Represents elements from the IMDB `cast_info` table.
  */
case class CastInfo(
    id: Integer,
    personId: Integer,
    movieId: Integer,
    personRoleId: Optional[Integer],
    note: Optional[String],
    nrOrder: Optional[Integer],
    roleId: Integer
) extends Serializable

object CastInfo extends Serializable {

  val fields = IndexedSeq("id", "person_id", "movie_id", "person_role_id", "note", "nr_order", "role_id")

  /**
    * Parse a CSV row into a [[CastInfo]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[CastInfo]]
    */
  def parseCsv(csv: String): CastInfo = {
    try {
      val csvFormat = CSVFormat.DEFAULT
          .withQuote('"')
          .withEscape('\\')
          .withIgnoreSurroundingSpaces(true)
          .builder()
          .build();
      val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

      CastInfo(
        fields(0).toInt,
        fields(1).toInt,
        fields(2).toInt,
        if (fields(3).nonEmpty) Optional.of(fields(3).toInt) else Optional.empty(),
        if (fields(4).nonEmpty) Optional.of(fields(4)) else Optional.empty(),
        if (fields(5).nonEmpty) Optional.of(fields(5).toInt) else Optional.empty(),
        fields(6).toInt
      )
    } catch {
      case _: Throwable => throw new Exception("Exception: " + (csv.map(c => s"[$c]").mkString(", ")))
    }
  }

  def toTuple(ci: CastInfo): (Integer, Integer, Integer, Optional[Integer], Optional[String], Optional[Integer], Integer) = {
    (ci.id, ci.personId, ci.movieId, ci.personRoleId, ci.note, ci.nrOrder, ci.roleId)
  }

  def toArray(ci: CastInfo): Array[AnyRef] = {
    Array(ci.id, ci.personId, ci.movieId, ci.personRoleId, ci.note, ci.nrOrder, ci.roleId)
  }
}

