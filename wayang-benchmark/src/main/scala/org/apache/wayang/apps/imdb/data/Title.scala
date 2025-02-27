package org.apache.wayang.apps.imdb.data

import java.util.Optional;
import org.apache.commons.csv._
import java.io.StringReader;
import scala.collection.JavaConverters._

/**
  * Represents elements from the IMDB `title` table.
  */
case class Title(
    id: Integer,
    title: String,
    imdbIndex: Optional[String],
    kindId: Integer,
    productionYear: Optional[Integer],
    imdbId: Optional[Integer],
    phoneticCode: Optional[String],
    episodeOfId: Optional[Integer],
    seasonNr: Optional[Integer],
    episodeNr: Optional[Integer],
    seriesYears: Optional[String],
    md5sum: Optional[String]
) extends Serializable

object Title extends Serializable {

  val fields = IndexedSeq("id", "title", "imdb_index", "kind_id", "production_year", "imdb_id", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "series_years", "md5sum")

  /**
    * Parse a CSV row into a [[Title]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Title]]
    */
  def parseCsv(csv: String): Title = {
    try {
      val csvFormat = CSVFormat.DEFAULT
          .withQuote('"')
          .withEscape('\\')
          .withIgnoreSurroundingSpaces(true)
          .builder()
          .build();
      val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

      Title(
        fields(0).toInt,
        fields(1),
        if (fields(2).nonEmpty) Optional.of(fields(2)) else Optional.empty(),
        fields(3).toInt,
        if (fields(4).nonEmpty) Optional.of(fields(4).toInt) else Optional.empty(),
        if (fields(5).nonEmpty) Optional.of(fields(5).toInt) else Optional.empty(),
        if (fields(6).nonEmpty) Optional.of(fields(6)) else Optional.empty(),
        if (fields(7).nonEmpty) Optional.of(fields(7).toInt) else Optional.empty(),
        if (fields(8).nonEmpty) Optional.of(fields(8).toInt) else Optional.empty(),
        if (fields(9).nonEmpty) Optional.of(fields(9).toInt) else Optional.empty(),
        if (fields(10).nonEmpty) Optional.of(fields(10)) else Optional.empty(),
        if (fields(11).nonEmpty) Optional.of(fields(11)) else Optional.empty()
      )
    } catch {
      case _: Throwable => throw new Exception("Exception: " + (csv.map(c => s"[$c]").mkString(", ")))
    }
  }

  def toTuple(t: Title): (Integer, String, Optional[String], Integer, Optional[Integer], Optional[Integer], Optional[String], Optional[Integer], Optional[Integer], Optional[Integer], Optional[String], Optional[String]) = {
    (t.id, t.title, t.imdbIndex, t.kindId, t.productionYear, t.imdbId, t.phoneticCode, t.episodeOfId, t.seasonNr, t.episodeNr, t.seriesYears, t.md5sum)
  }

  def toArray(t: Title): Array[AnyRef] = {
    Array(t.id, t.title, t.imdbIndex, t.kindId, t.productionYear, t.imdbId, t.phoneticCode, t.episodeOfId, t.seasonNr, t.episodeNr, t.seriesYears, t.md5sum)
  }
}

