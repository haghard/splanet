package com.gateway.server

import io.vertx.rxcore.java.eventbus.RxMessage
import org.vertx.java.core.json.{JsonArray, JsonObject}

object MessageParsers {

  trait MongoResponseFieldParser0 extends ((RxMessage[JsonObject], String) => Option[String]) {
    override def apply(message: RxMessage[JsonObject], fieldName: String): Option[String] = {
      val array = for {
        v <- Option(message.body().getArray(SportPlanetService.MONGO_RESULT_FIELD))
        if (v.size() > 0)
      } yield v.get(0).asInstanceOf[JsonObject].getArray(fieldName)
      if (array.isDefined)
        Some(array.get.toArray.mkString(","))
      else None
    }
  }

  trait MongoResponseParser1 extends (RxMessage[JsonObject] => Option[String]) {
    override def apply(message: RxMessage[JsonObject]): Option[String] = {
      for {
        v <- Option(message.body().getArray(SportPlanetService.MONGO_RESULT_FIELD));
        if (v.size() > 0)
      } yield v.toString.replaceAll("\\[|]", "")
    }
  }

  trait MongoResponseFieldParser2 extends ((RxMessage[JsonObject], String) => Option[JsonArray]) {
    override def apply(message: RxMessage[JsonObject], fieldName: String): Option[JsonArray] = {
      for {
        v <- Option(message.body().getArray(SportPlanetService.MONGO_RESULT_FIELD))
        if (v.size() > 0)
      } yield v.get(0).asInstanceOf[JsonObject].getArray(fieldName)
    }
  }

  object ResponseFieldParser extends MongoResponseFieldParser0
  object ResponseParser extends MongoResponseParser1
  object ResponseFieldParserToArray extends MongoResponseFieldParser2
}