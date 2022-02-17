package utils

import scala.reflect.runtime.universe._

object ObjectFactory {

  def apply[T: TypeTag](fieldValuesProvider: FieldValuesProvider): T = {

    val mirror = scala.reflect.runtime.currentMirror

    val applyMethod = typeOf[T].companion.decl(TermName("apply")).asMethod
    val params = applyMethod.paramLists.head
    val obj =
      mirror.reflectModule(typeOf[T].typeSymbol.companion.asModule).instance

    val args = (1 to params.length).map(paramIndex => {
      val param = params(paramIndex - 1)
      val paramName = param.name.decodedName.toString.toLowerCase

      param.info match {
        case paramType if paramType =:= typeOf[String] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsString(paramName), mirror, obj, paramIndex)
        case paramType if paramType =:= typeOf[Option[String]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsString(paramName), mirror, obj, paramIndex)
        case paramType if paramType =:= typeOf[Option[BigDecimal]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsBigDecimal(paramName), mirror, obj, paramIndex)
        case paramType if paramType =:= typeOf[BigDecimal] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsBigDecimal(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Option[Long]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsLong(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Long] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsLong(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Option[Int]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsInt(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Int] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsInt(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Option[Double]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsDouble(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Double] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsDouble(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[java.sql.Date] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsDate(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Option[java.sql.Date]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsDate(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Option[java.sql.Timestamp]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsTimestamp(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[java.sql.Timestamp] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsTimestamp(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Option[Boolean]] =>
          getCellValueAsOption[T](fieldValuesProvider.getCellValueAsBoolean(paramName), mirror, obj, paramIndex)
        case paramType if paramType <:< typeOf[Boolean] =>
          getCellValue[T](fieldValuesProvider.getCellValueAsBoolean(paramName), mirror, obj, paramIndex)
        case paramType => default(paramType, paramName, fieldValuesProvider)
      }
    })

    mirror
      .reflect(obj)
      .reflectMethod(applyMethod)(args: _*)
      .asInstanceOf[T]
  }

  private def getCellValueAsOption[T: TypeTag](paramValue: Option[Any],
                                               mirror: Mirror,
                                               instance: Any,
                                               index: Int): Any = {
    if (paramValue.isEmpty) {
      getFieldDefaultValue[T](mirror, instance, index)
    }
    paramValue
  }

  private def getCellValue[T: TypeTag](paramValue: Option[Any], mirror: Mirror, instance: Any, index: Int): Any = {
    paramValue.getOrElse(getFieldDefaultValue[T](mirror, instance, index))
  }

  private def default(t: Type, paramName: String, fieldValuesProvider: FieldValuesProvider): Any = {
    val finalObject = fieldValuesProvider.getPojo(paramName)

    finalObject.orNull
  }

  private def getFieldDefaultValue[T: TypeTag](mirror: Mirror, instance: Any, index: Int): Any = {
    val defaultArgument =
      typeOf[T].companion.decl(TermName(s"apply$$default$$$index"))
    if (defaultArgument != NoSymbol) {
      return scala.reflect.runtime.currentMirror
        .reflect(instance)
        .reflectMethod(defaultArgument.asMethod)
        .apply()
    }
    null
  }

}
