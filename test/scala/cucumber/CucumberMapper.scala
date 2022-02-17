package cucumber

import scala.reflect.runtime.universe._
import gherkin.pickles.PickleRow
import utils.{FieldValuesProvider, ObjectFactory}

class CucumberMapper[T: TypeTag](rows: Iterable[PickleRow], defaultFields: Seq[Iterable[PickleRow]] = Seq.empty)
  extends CucumberObjectMapper[T] {

  override def getRows: Iterable[PickleRow] = rows

  override def createObject(): T = createSequence().head

  override def createSequence(): Seq[T] = {
    val defFields = createDefaultFields()
    getData
      .map(row => {
        val valueProvider: FieldValuesProvider = getValueProvider(row, defFields)
        ObjectFactory[T](valueProvider)
      })
      .toSeq
  }

  private def getValueProvider(row: PickleRow, defaultFieldsMap: Map[String, String]) = {
    CucumberUtils.createValueProviderWithDefault(row, CucumberUtils.getIndexToNameMap(rows), defaultFieldsMap)
  }

  private def createDefaultFields(): Map[String, String] = {
    defaultFields
      .flatMap(
        defaultValueIterator =>
          dropHeader(defaultValueIterator)
            .map(
              row => CucumberUtils.getFieldValuesMapFromRow(row, CucumberUtils.getIndexToNameMap(defaultValueIterator))
            )
            .headOption)
      .flatten
      .toMap
  }

  override def createObjectAsOptional(): Option[T] = createSequence().headOption

  def createList(): List[T] = createSequence().toList

  def getDataAsList(rows: Iterable[PickleRow]): List[PickleRow] = {
    dropHeader(rows).toList
  }

  private def dropHeader(pickleRows: Iterable[PickleRow]): Iterable[PickleRow] = {
    pickleRows.drop(1)
  }

  def getData: Iterable[PickleRow] = {
    dropHeader(rows)
  }

  def mergeRows(newFieldRows: Iterable[PickleRow]): Seq[T] = {
    val originalIndexToNameMap = CucumberUtils.getIndexToNameMap(rows)
    val newFieldIndexToNameMap = CucumberUtils.getIndexToNameMap(newFieldRows)

    val originalRowData = getDataAsList(rows)
    val newFieldRowData = getDataAsList(newFieldRows)

    (0 until getData.size)
      .map(index => {
        ObjectFactory[T](
          CucumberUtils.createMergedValuesProvider(originalRowData(index),
            originalIndexToNameMap,
            newFieldRowData(index),
            newFieldIndexToNameMap))
      })
  }

  def createMergedObject(newFieldRows: Iterable[PickleRow]): T =
    mergeRows(newFieldRows).head

}
