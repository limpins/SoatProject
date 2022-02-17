package cucumber

import gherkin.pickles.PickleRow

trait CucumberObjectMapper[T] {

  def createObject(): T

  def createObjectAsOptional(): Option[T]

  def getRows: Iterable[PickleRow]

  def getData: Iterable[PickleRow]

  def createList(): List[T]

  def createSequence(): Seq[T]
}

