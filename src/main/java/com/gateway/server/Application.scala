package com.gateway.server

import org.vertx.java.core.http.HttpServer
import io.vertx.rxcore.java.eventbus.RxEventBus
import org.vertx.java.core.logging.Logger
import com.escalatesoft.subcut.inject.NewBindingModule
import NewBindingModule.newBindingModule
import com.gateway.server.exts._
import org.vertx.java.core.json.JsonObject
import com.gateway.server.actors.ScraperApplication
import com.typesafe.config.ConfigFactory

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

    val config = ConfigFactory load

    val scraperModule = newBindingModule {
      module =>
        import module._
        import collection.JavaConversions._
        import scala.concurrent.duration._

        bind[List[String]].toSingle(config.getStringList("teams").toList)
        bind[String].idBy(ScraperUrl).toSingle(config getString ("url"))
        bind[String].idBy(ScraperStatCollection).toSingle(config getString ("statCollection"))
        bind[MongoConfig].toSingle(MongoConfig(persistCfg getString("host"), persistCfg.getNumber("port").intValue, persistCfg getString("db_name")))

        bind[FiniteDuration].idBy(ScraperDelay).toSingle(config getInt ("scrapPeriodInHour") seconds)
        bind[FiniteDuration].idBy(ScraperPeriod).toSingle(config getInt ("scrapPeriodInHour") hours)
    }

    implicit val spModule = newBindingModule { module =>
        module <~ httpModule
        module <~ scraperModule
    }

    new SportPlanetService().start
    new ScraperApplication().start
  }
}
