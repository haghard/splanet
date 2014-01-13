package com.gateway.server.actors

import org.jsoup.nodes.Element

trait ParserImplicits {

  import scala.collection.convert.WrapAsScala._

  implicit class ElementExtensions(val element: Element) {
    def oneByClass(className: String): Option[Element] = element.getElementsByClass(className).toList.headOption

    def oneByTag(tagName: String): Option[Element] = element.getElementsByTag(tagName).toList.headOption

    def byId(id: String): Option[Element] = Option(element.getElementById(id))
  }

}
