package com.gateway.server

import org.vertx.java.core.http.HttpServer
import io.vertx.rxcore.java.eventbus.RxEventBus
import org.vertx.java.core.logging.Logger
import com.escalatesoft.subcut.inject.NewBindingModule
import NewBindingModule.newBindingModule
import com.gateway.server.exts._
import org.vertx.java.core.json.JsonObject
import com.gateway.server.actors.ScraperApplication

class Application(val server: HttpServer, val bus: RxEventBus, val persistCfg: JsonObject, val logger: Logger, val httpPort: Int) {

  def start = {

    val httpModule = newBindingModule {
      module =>
        import module._
        bind[HttpServer] toSingle (server)
        bind[RxEventBus] toSingle (bus)
        bind[Logger] toSingle (logger)

        bind[String].idBy(MongoPersistorKey).toSingle("mongo-persistor")
        bind[String].idBy(MongoResponseArrayKey).toSingle("results")

        bind[Int].idBy(HttpServerPort).toSingle(httpPort)
    }

    val mongoModule = newBindingModule {
      module =>
        import module._
        bind[String].idBy(MongoHost).toSingle(persistCfg getString("host"))
        bind[Int].idBy(MongoPort).toSingle(persistCfg.getNumber("port").intValue)
        bind[String].idBy(MongoDBName).toSingle(persistCfg getString("db_name"))
    }

    implicit val spModule = newBindingModule { module =>
        module <~ httpModule
        module <~ mongoModule
    }

    new SportPlanetService().start
    new ScraperApplication().start
  }
}
