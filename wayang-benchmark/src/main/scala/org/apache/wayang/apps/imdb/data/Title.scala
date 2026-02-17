package org.apache.wayang.apps.imdb.data

import java.util.Optional;
import org.apache.commons.csv._
import java.io.StringReader;
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

/**
  * Represents elements from the IMDB `title` table.
  */
case class Title(
    id: Integer,
    title: String,
    imdbIndex: String,
    kindId: Integer,
    productionYear: Integer,
    imdbId: String,
    phoneticCode: String,
    episodeOfId: Integer,
    seasonNr: Integer,
    episodeNr: Integer,
    seriesYears: String,
    md5sum: String
) extends Serializable

object Title extends Serializable {

  //val fields = IndexedSeq("id", "title", "imdb_index", "kind_id", "production_year", "imdb_id", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "series_years", "md5sum")

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
          .withIgnoreEmptyLines(false)
          .withNullString("")   // treat empty fields as null
          .builder()
          .build();
      val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

      Title(
        get(fields, 0).toInt,
        get(fields, 1),
        optStr(fields, 2),
        get(fields, 3).toInt,
        optInt(fields, 4),
        optStr(fields, 5),
        optStr(fields, 6),
        optInt(fields, 7),
        optInt(fields, 8),
        optInt(fields, 9),
        optStr(fields, 10),
        optStr(fields, 11)
      )
    } catch {
      case e: Throwable => throw e
        //throw new Exception("Exception: " + (csv.map(c => s"[$c]").mkString(", ")))
    }
  }

  def toTuple(t: Title): (Integer, String, String, Integer, Integer, String, String, Integer, Integer, Integer, String, String) = {
    (t.id, t.title, t.imdbIndex, t.kindId, t.productionYear, t.imdbId, t.phoneticCode, t.episodeOfId, t.seasonNr, t.episodeNr, t.seriesYears, t.md5sum)
  }

  def toArray(t: Title): Array[AnyRef] = {
    Array(t.id, t.title, t.imdbIndex, t.kindId, t.productionYear, t.imdbId, t.phoneticCode, t.episodeOfId, t.seasonNr, t.episodeNr, t.seriesYears, t.md5sum)
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

