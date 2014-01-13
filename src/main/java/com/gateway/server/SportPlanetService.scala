package com.gateway.server

import org.vertx.java.core.http.{HttpServerRequest, RouteMatcher, HttpServer}
import org.vertx.java.core.logging.Logger
import java.io.File
import org.vertx.java.core.buffer.Buffer
import com.google.common.hash.Hashing
import com.google.common.base.Charsets
import org.vertx.java.core.json.JsonObject
import io.vertx.rxcore.java.eventbus.{RxMessage, RxEventBus}
import java.util.concurrent.atomic.AtomicInteger
import com.gateway.server.exts._
import MessageParsers._
import QMongo._

object SportPlanetService {

  val USER_EMAIL = "email"

  val USER_PASSWORD = "password"

  val MONGO_RESULT_FIELD = "results"

  val MONGO_MODULE_NAME = "mongo-persistor"

  val LOGIN_PAGE = "/examples/sportPlanet/login.html"

  val REPORTS_PAGE = "/examples/sportPlanet/center.html"

  val LOGIN_FAILED_PAGE = "/examples/sportPlanet/login_failed.html";

  val WEBROOT = new File(".").getAbsolutePath.replace(".", "") + "web/bootstrap"

  def hash(password: String) = {
    Hashing.md5.newHasher.putString(password, Charsets.UTF_8).hash.toString
  }

  def apply(server: HttpServer, bus: RxEventBus, logger: Logger) = new SportPlanetService(server, bus, logger).config()
}

class SportPlanetService(val server: HttpServer, val eventBus: RxEventBus, val logger: Logger) {
  import SportPlanetService._

  def config() = {
    val router = new RouteMatcher

    router.getWithRegEx(".*", { req: HttpServerRequest =>
        req.path match {
          case "/" => req.response.sendFile(SportPlanetService.WEBROOT + SportPlanetService.LOGIN_PAGE)
          case r =>
            if (r.contains("signin.css") || r.contains("client.js") || r.contains("app.js")) {
              req.response.sendFile(SportPlanetService.WEBROOT + "/examples/sportPlanet" + req.path)
            } else {
              req.response.sendFile(SportPlanetService.WEBROOT + req.path)
            }
        }
    })

    import exts.fnToHandler1
    router.post("/center", fnToHandler1 { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler({ buffer: Buffer =>
        val reqLine = buffer.getString(0, buffer.length)
        if (!reqLine.matches("email=[\\w]+&password+=[\\w]+"))
          throw IllegalHttpReqParams("auth params not match with pattern email=[w]+&password+=[w]+")

        val email = req.formAttributes().get(USER_EMAIL)
        val passwordHash = SportPlanetService.hash(req.formAttributes().get(USER_PASSWORD))

        import exts.{ fnToFunc1, fnToAction1 }
        val temp = eventBus.send[JsonObject, JsonObject](MONGO_MODULE_NAME, createUserQuery(passwordHash, email)).flatMap { mes: RxMessage[JsonObject] =>
          val idArray = parseMessageToArray(mes, "followedTeams")
          if (idArray.isDefined) { eventBus.send[JsonObject, JsonObject](MONGO_MODULE_NAME, followedTeams(idArray.get.iterator)) }
          else { throw DBAccessException create(s"Response parse error [USER] : ${mes.body}") }
        }.subscribe( { mes: RxMessage[JsonObject] =>
            val resultLine = parseTeamMessage(mes)
            req.response.headers.add("Set-Cookie", "auth-user=" + email)
            req.response.headers.add("Set-Cookie", "followed-teams=" + resultLine.toString)
            req.response.sendFile(WEBROOT + REPORTS_PAGE)
        }, {
          th: Throwable => logger.info(th.getMessage)
          req.response.sendFile(WEBROOT + LOGIN_FAILED_PAGE)
        })
      })
    })

    import exts.fnToFunc1
    router.post("/recent", fnToHandler1 { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler { buffer: Buffer =>
        val reqLine = buffer.getString(0, buffer.length).replace('+', ' ')
        val cleanTeams = List(reqLine.split("&"): _*).map({ line => line.substring(line.indexOf('=') + 1, line.length)})
        val responseWriter = new ChunkedResponseWriter(req, new AtomicInteger(cleanTeams.size))

        val r = cleanTeams.map({ teamName =>
          eventBus.send[JsonObject, JsonObject](MONGO_MODULE_NAME, recentResultByTeam(teamName))
        }) .map ({ ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.flatMap({ mes: RxMessage[JsonObject] =>
            val gamesId = parseMessageToArray(mes, "games_id")
            if (gamesId.isDefined) { eventBus.send[JsonObject, JsonObject](MONGO_MODULE_NAME, recentResultsById(gamesId.get)) }
            else { req.response.end; throw DBAccessException.create(s"Response parse error [RECENT GAMES ID] : ${mes.body}") }
          })
        }).map { ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.subscribe( { mes: RxMessage[JsonObject] =>

            val responseChunk = parseCompleteRecentResults(mes)
            if (responseChunk.isEmpty) {
              req.response.end; DBAccessException.create(s"Response parse error [RECENT GAMES] : ${mes.body}")
            }

            responseWriter.write(responseChunk.get)
          },
          { th: Throwable => logger.info(th.getMessage) }
          )
        }
      }
    })

    server.requestHandler(router)
  }

  class DBAccessException(msg: String, th : Throwable) extends Exception(msg, th)
  case class IllegalHttpReqParams(msg: String) extends Exception(msg)

  object DBAccessException {
    def create(msg: String) = new DBAccessException(msg, null)
    def create(msg: String, cause: Throwable) = new DBAccessException(msg, cause)
  }
}
