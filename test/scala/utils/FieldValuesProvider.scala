package utils

import java.sql.{Date, Timestamp}
import scala.util.{Failure, Success, Try}
import utils.DateUtils._

class FieldValuesProvider(dataTableFields: Map[String, String], pojoMap: Option[Map[String, Any]] = None) {

  def getPojo(fieldName: String): Option[Any] = {
    pojoMap.flatMap(_.get(fieldName))
  }

  def getCellValueAsDate(fieldName: String): Option[Date] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(convertStringToDate(value)))
  }

  def getCellValueAsTimestamp(fieldName: String): Option[Timestamp] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(convertStringToTimestamp(value)))
  }

  def getCellValueAsLong(fieldName: String): Option[Long] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(mapFieldNameToLong(value)))
  }

  private def mapFieldNameToLong(value: String): Long = {
    Try(value.toLong) match {
      case Success(longValue) => longValue
      case Failure(message)   => getEpochTimeMsFromDateOrTimestamp(Some(value))
    }
  }

  private def getEpochTimeMsFromDateOrTimestamp(date: Option[String]): Long = {
    Try(getEpochTimeMsFromTimestamp(date)) match {
      case Success(epochTime) => epochTime
      case Failure(message) => {
        getEpochTimeMsFromDate(date)
      }
    }
  }

  def getCellValueAsString(fieldName: String): Option[String] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(value))
  }

  def getCellValueAsBigDecimal(fieldName: String): Option[BigDecimal] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => {
        Some(BigDecimal(value))
      })
  }

  def getCellValueAsInt(fieldName: String): Option[Int] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(value.toInt))
  }

  def getCellValueAsDouble(fieldName: String): Option[Double] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(value.toDouble))
  }

  def getCellValueAsBoolean(fieldName: String): Option[Boolean] = {
    dataTableFields
      .get(fieldName)
      .flatMap(value => Some(value.toBoolean))
  }
}

