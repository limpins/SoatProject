package cucumber

import cucumber.api.DataTable
import gherkin.pickles.PickleRow
import utils.FieldValuesProvider

object CucumberUtils {

  def getRows(datable: DataTable): Iterable[PickleRow] = {
    import scala.collection.JavaConverters._
    datable.getPickleRows.asScala
  }

  def getIndexToNameMap(rows: Iterable[PickleRow]): Map[Int, String] = {
    import scala.collection.JavaConverters._

    val headerColumns = rows.head.getCells.asScala

    (0 to headerColumns.length - 1)
      .map(index => (index, headerColumns(index).getValue))
      .toMap
  }

  def createValueProvider(row: PickleRow,
                          header: Map[Int, String],
                          pojoMap: Option[Map[String, Any]] = None): FieldValuesProvider = {
    val fieldValues = getFieldValuesMapFromRow(row, header)
    new FieldValuesProvider(fieldValues, pojoMap)
  }

  def createValueProviderWithDefault(row: PickleRow,
                                     header: Map[Int, String],
                                     defaultValues: Map[String, String]): FieldValuesProvider = {
    val fieldValues = defaultValues ++ getFieldValuesMapFromRow(row, header)
    new FieldValuesProvider(fieldValues)
  }

  def createValuesProviderForTransposed(rows: Iterable[PickleRow],
                                        pojoMap: Option[Map[String, Any]] = None): List[FieldValuesProvider] = {
    val s = rows.head.getCells.size()

    (1 until s).map { index =>
      new FieldValuesProvider(rows.map { row =>
        row.getCells.get(0).getValue.toLowerCase -> row.getCells.get(index).getValue
      } toMap, pojoMap)
    }.toList

  }

  def createMergedValuesProvider(originalRow: PickleRow,
                                 originalHeader: Map[Int, String],
                                 newFieldsRow: PickleRow,
                                 newFieldHeader: Map[Int, String]): FieldValuesProvider = {

    val originalFieldValues =
      getFieldValuesMapFromRow(originalRow, originalHeader)
    val newFieldsValues = getFieldValuesMapFromRow(newFieldsRow, newFieldHeader)
    val mergedSequence = originalFieldValues.toSeq ++ newFieldsValues.toSeq
    val mergedMap = mergedSequence.groupBy(_._1).mapValues(_.map(_._2).last)
    new FieldValuesProvider(mergedMap)
  }

  def getFieldValuesMapFromRow(row: PickleRow, header: Map[Int, String]): Map[String, String] = {
    import scala.collection.JavaConverters._
    val cellsList = row.getCells.asScala
    val cellsListWithIndex = (0 to cellsList.length - 1).map(index => (index, cellsList(index)))

    cellsListWithIndex
      .filter(cell => cell._2.getValue != "")
      .map(
        cell =>
          (header
            .getOrElse(cell._1, throw new RuntimeException(s"cannot find header name for column with index ${cell._1}"))
            .toLowerCase,
            cell._2.getValue))
      .toMap
  }

}

