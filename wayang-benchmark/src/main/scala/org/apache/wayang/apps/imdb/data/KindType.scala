package org.apache.wayang.apps.imdb.data

import java.util.Optional;
import org.apache.commons.csv._
import java.io.StringReader;
import scala.collection.JavaConverters._

/**
  * Represents elements from the IMDB `kind_type` table.
  */
case class KindType(
    id: Integer,
    kind: String
) extends Serializable

object KindType extends Serializable {

  val fields = IndexedSeq("id", "kind")

  /**
    * Parse a CSV row into a [[KindType]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[KindType]]
    */
  def parseCsv(csv: String): KindType = {
    try {
      val csvFormat = CSVFormat.DEFAULT
          .withQuote('"')
          .withEscape('\\')
          .withIgnoreSurroundingSpaces(true)
          .builder()
          .build();
      val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

      KindType(
        fields(0).toInt,
        fields(1)
      )
    } catch {
      case _: Throwable => throw new Exception("Exception: " + (csv.map(c => s"[$c]").mkString(", ")))
    }
  }

  def toTuple(kt: KindType): (Integer, String) = {
    (kt.id, kt.kind)
  }

  def toArray(kt: KindType): Array[AnyRef] = {
    Array(kt.id, kt.kind)
  }
}

