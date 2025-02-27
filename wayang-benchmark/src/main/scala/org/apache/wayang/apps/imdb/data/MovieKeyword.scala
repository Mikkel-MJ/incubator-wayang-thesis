package org.apache.wayang.apps.imdb.data

import java.util.Optional;
import org.apache.commons.csv._
import java.io.StringReader;
import scala.collection.JavaConverters._

/**
  * Represents elements from the IMDB `movie_keyword` table.
  */
case class MovieKeyword(
    id: Integer,
    movieId: Integer,
    keywordId: Integer
) extends Serializable

object MovieKeyword extends Serializable {

  val fields = IndexedSeq("id", "movie_id", "keyword_id")

  /**
    * Parse a CSV row into a [[MovieKeyword]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[MovieKeyword]]
    */
  def parseCsv(csv: String): MovieKeyword = {
    try {
      val csvFormat = CSVFormat.DEFAULT
          .withQuote('"')
          .withEscape('\\')
          .withIgnoreSurroundingSpaces(true)
          .builder()
          .build();
      val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

      MovieKeyword(
        fields(0).toInt,
        fields(1).toInt,
        fields(2).toInt
      )
    } catch {
      case _: Throwable => throw new Exception("Exception: " + (csv.map(c => s"[$c]").mkString(", ")))
    }
  }

  def toTuple(mk: MovieKeyword): (Integer, Integer, Integer) = {
    (mk.id, mk.movieId, mk.keywordId)
  }

  def toArray(mk: MovieKeyword): Array[AnyRef] = {
    Array(mk.id, mk.movieId, mk.keywordId)
  }
}

