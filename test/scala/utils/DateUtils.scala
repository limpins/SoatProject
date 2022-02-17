package utils

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.TimeZone
import scala.util.Try

object DateUtils {

  val dateTimePattern = "yyyy-MM-dd HH:mm:ss"
  val datePatterns = Seq("yyyy-MM-dd", "dd-MMM-yy")
  val timeStampFormatter = new SimpleDateFormat(dateTimePattern)
  val simpleDateFormatter = new SimpleDateFormat(datePatterns.head)

  val timestampFormatter = DateTimeFormatter.ofPattern(dateTimePattern).withZone(TimeZone.getDefault().toZoneId)

  def convertStringToTimestamp(date: String): Timestamp = {
    new Timestamp(timeStampFormatter.parse(date).getTime)
  }

  def convertStringToDate(dateStr: String): Date = {
    datePatterns.view
      .map(df => Try { new Date(new SimpleDateFormat(df).parse(dateStr).getTime) })
      .find(_.isSuccess)
      .flatMap(_.toOption)
      .getOrElse(Date.valueOf(LocalDate.now))
  }

  def getEpochTimeMsFromTimestamp(date: Option[String]): Long = {
    getEpochTimeSecondsFromTimestamp(date) * 1000
  }

  def getEpochTimeSecondsFromTimestamp(date: Option[String]): Long = {
    date
      .flatMap(
        dt => {
          val zdt = ZonedDateTime.parse(dt, timestampFormatter);
          Some(zdt.toEpochSecond)
        }
      )
      .getOrElse(0)
  }

  def getEpochTimeMsFromDate(date: Option[String]): Long = {
    getEpochTimeDayFromDate(date) * 86400000
  }

  def getEpochTimeDayFromDate(dateStr: Option[String]): Long = {
    datePatterns
      .map(df =>
        dateStr.flatMap(d =>
          Try { LocalDate.parse(d, DateTimeFormatter.ofPattern(df).withZone(TimeZone.getDefault().toZoneId)) } toOption))
      .find(_.isDefined)
      .flatten
      .flatMap(zdt => Some(zdt.toEpochDay()))
      .getOrElse(0)

  }

}
