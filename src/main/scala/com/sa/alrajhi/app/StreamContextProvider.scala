package com.sa.alrajhi.app

abstract class StreamContextProvider[+A: Manifest] extends EnvironmentContextProvider {

  override def withConfiguration[C: Manifest](newConfiguration: C): StreamContextProvider[C] = {
    implicit val contextProvider: StreamContextProvider[A] = this

    StreamContextProvider.withConfiguration[C](newConfiguration)
  }
}

object StreamContextProvider {

  def withConfiguration[C: Manifest](newConfiguration: C)(implicit contextProvider: StreamContextProvider[_]): StreamContextProvider[C] = {
    val sourceContextPrefix = contextProvider.contextPrefix
    lazy val sourceContextConfigString = contextProvider.configString

    new StreamContextProvider[C] {
      override def hasAttribute(attribute: String): Boolean = contextProvider.hasAttribute(attribute)
      override def getAttribute(attribute: String): String = contextProvider.getAttribute(attribute)
      override val contextPrefix: Option[String] = sourceContextPrefix
      override lazy val configString: String = sourceContextConfigString


    }
  }
}
