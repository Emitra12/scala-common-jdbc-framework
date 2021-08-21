package com.sa.alrajhi.app

trait ContextProvider {
  /***
   * if all variables should have some prefix separated with a dot
   */
  protected val contextPrefix: Option[String] = None
  protected def attributeTransformer(attribute: String): String = attribute
  protected def getAttributeName(attribute: String): String = attributeTransformer(contextPrefix.map(o => s"$o.$attribute").getOrElse(attribute))

  /***
   * @return the value of the attribute if exists
   */
  @throws(classOf[RuntimeException])
  def getAttributeAsOption(attribute: String): Option[String] = if(!hasAttribute(getAttributeName(attribute))) None else Some(getAttribute(getAttributeName(attribute)))

  /***
   * @param attribute the attribute
   * @return checks if a certain attribute exists
   */
  def hasAttribute(attribute: String): Boolean = ???

  /***
   * gets the value of an attribute
   *
   * @throws java.lang.RuntimeException
   * @return the value of the attribute
   */
  @throws(classOf[RuntimeException])
  def getAttribute(attribute: String): String = ???
}
