package com.gateway.server

import io.vertx.rxcore.java.eventbus.RxMessage
import org.vertx.java.core.json.{JsonArray, JsonObject}
import java.lang.StringBuilder

object MessageParsers {

  def parseTeamMessage(message: RxMessage[JsonObject]): StringBuilder = {
    var f = true
    val resultLine = new StringBuilder
    val result = for {v <- Option(message.body().getArray(SportPlanetService.MONGO_RESULT_FIELD))} yield { v }

    import scala.collection.JavaConversions.asScalaIterator
    val rs: Iterator[AnyRef] = result.get.iterator()
    for (res <- rs) {
      if (f) {
        resultLine.append(res.asInstanceOf[JsonObject].getString("name"))
        f = false
      } else {
        resultLine.append(",").append(res.asInstanceOf[JsonObject].getString("name"))
      }
    }
    resultLine
  }

  def parseMessageToArray(message: RxMessage[JsonObject], name: String): Option[JsonArray] = {
    for {
      v <- Option(message.body().getArray(SportPlanetService.MONGO_RESULT_FIELD))
      if (v.size() > 0)
    } yield v.get(0).asInstanceOf[JsonObject].getArray(name)
  }

  def parseCompleteRecentResults(message: RxMessage[JsonObject]): Option[String] = {
    for {
      v <- Option(message.body().getArray(SportPlanetService.MONGO_RESULT_FIELD));
      if (v.size() > 0)
    } yield v.toString.replaceAll("\\[|]", "")
  }
}