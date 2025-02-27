package org.apache.wayang.apps.imdb.data

import java.util.Optional;
import org.apache.commons.csv._
import java.io.StringReader;
import scala.collection.JavaConverters._

/**
  * Represents elements from the IMDB `movie_link` table.
  */
case class MovieLink(
    id: Integer,
    movieId: Integer,
    linkedMovieId: Integer,
    linkTypeId: Integer
) extends Serializable

object MovieLink extends Serializable {

  val fields = IndexedSeq("id", "movie_id", "linked_movie_id", "link_type_id")

  /**
    * Parse a CSV row into a [[MovieLink]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[MovieLink]]
    */
  def parseCsv(csv: String): MovieLink = {
    try {
      val csvFormat = CSVFormat.DEFAULT
          .withQuote('"')
          .withEscape('\\')
          .withIgnoreSurroundingSpaces(true)
          .builder()
          .build();
      val fields = csvFormat.parse(new StringReader(s"""$csv""")).getRecords().get(0).toList.asScala;

      MovieLink(
        fields(0).toInt,
        fields(1).toInt,
        fields(2).toInt,
        fields(3).toInt
      )
    } catch {
      case _: Throwable => throw new Exception("Exception: " + (csv.map(c => s"[$c]").mkString(", ")))
    }
  }

  def toTuple(ml: MovieLink): (Integer, Integer, Integer, Integer) = {
    (ml.id, ml.movieId, ml.linkedMovieId, ml.linkTypeId)
  }

  def toArray(ml: MovieLink): Array[AnyRef] = {
    Array(ml.id, ml.movieId, ml.linkedMovieId, ml.linkTypeId)
  }
}

