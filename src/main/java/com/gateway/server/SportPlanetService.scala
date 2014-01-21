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
import QMongo._
import java.net.{URLEncoder, URLDecoder}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}

object SportPlanetService {

  val USER_EMAIL = "email"

  val USER_PASSWORD = "password"

  val LOGIN_PAGE = "/examples/sportPlanet/login.html"

  val REPORTS_PAGE = "/examples/sportPlanet/center.html"

  val LOGIN_FAILED_PAGE = "/examples/sportPlanet/login_failed.html";

  val WEBROOT = new File(".").getAbsolutePath.replace(".", "") + "web/bootstrap"

  def hash(password: String) = {
    Hashing.md5.newHasher.putString(password, Charsets.UTF_8).hash.toString
  }
}

import SportPlanetService._
import com.gateway.server.exts.MongoPersistorKey

class SportPlanetService(implicit val bindingModule: BindingModule) extends Injectable {
  val server = inject [HttpServer]
  val rxEventBus = inject [RxEventBus]
  val logger = inject [Logger]

  val pModule = inject [String](MongoPersistorKey)
  val port = inject [Int](HttpServerPort)

  def start = {
    val router = new RouteMatcher

    /**
     * Rest resource /recent-stat/:team
     *
     **/
    import exts.fnToFunc1
    router get("/recent-stat/:team", fnToHandler1 { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        val teamName = URLDecoder decode(req.params.get("team"), "UTF-8")
        rxEventBus.send(pModule, recentStat(teamName, 10)).subscribe { mes: RxMessage[JsonObject] =>
          val statistic = RecentHealthProcessor(mes, teamName)
          if (statistic.isDefined) req.response.end(RecentHealthProcessor(mes, teamName).get.toString)
          else req.response.end("{}")
        }
      }
    })

    router get("/auth", fnToHandler1 { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        val email = req.params.get("email")
        val passwordHash = hash(req.params.get("password"))
        rxEventBus.send[JsonObject, JsonObject](pModule, createUserQuery(passwordHash, email))
          .subscribe({ mes: RxMessage[JsonObject] =>
            val followedTeams = ResponseFieldParser(mes, "followedTeams")
            val ft = URLEncoder.encode(followedTeams getOrElse(
                throw DBAccessException create(s"Response parse error [USER-followedTeams] : ${mes.body}")), "UTF-8")

          req.response.end(new JsonObject()
            .putString("status", "ok")
            .putString("user", email)
            .putString("followed-teams", ft).toString)
        }, {
          th: Throwable => logger.info(th.getMessage)
            req.response.end(new JsonObject().putString("status","error").toString)
        })
      }
    })
    /**
     * Rest resource /recent/:followedTeam
     * Return recent games by single team
     *
     * JS simple client:
     *  function fetchRecent(team) {
     *   var promise = $.ajax({
     *       url: 'http://192.168.0.143:9000/recent/' + team
     *   }).promise();
     *   return Rx.Observable.fromPromise(promise);
     */
    router.get("/recent/:followedTeam", fnToHandler1 { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler { buffer: Buffer =>
        val teamName = URLDecoder decode(req.params.get("followedTeam"), "UTF-8")
        val responseWriter = new ChunkedResponseWriter(req, new AtomicInteger(1))

        val r = List(teamName).map({ teamName =>
          rxEventBus.send[JsonObject, JsonObject](pModule, recentResultByTeam(teamName))
        }) .map ({ ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.flatMap({ mes: RxMessage[JsonObject] =>
            val gamesId = ResponseFieldParserToArray(mes, "games_id")
            if (gamesId.isDefined) { rxEventBus.send[JsonObject, JsonObject](pModule, recentResultsById(gamesId.get)) }
            else { req.response.end; throw DBAccessException.create(s"Response parse error [RECENT GAMES ID] : ${mes.body}") }
          })
        }) .map { ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.subscribe( { mes: RxMessage[JsonObject] =>
            val responseChunk = ResponseParser(mes)
            if (responseChunk.isEmpty) {
              req.response.end; DBAccessException.create(s"Response parse error [RECENT GAMES] : ${mes.body}")
            } else {
              responseWriter.write(responseChunk.get)
            }
          },
          { th: Throwable => logger.info(th.getMessage) }
          )
        }
      }
    })

    /**
     *  Return recent games by many teams
     *  passed as data parameter
     *
     *  JS simple client:
     *  function fetchRecent(teams) {
     *   var promise = $.ajax({
     *       url: 'http://192.168.0.143:9000/recent'
     *       data: 'followed-teams=' + teams
     *   }).promise();
     *   return Rx.Observable.fromPromise(promise);
     *
     *   teams - is a names separated by ,
     *   http://localhost:9000/recent?followed-teams=Chicago+Bulls%2CMiami+Heat
     *}
     */
    router.get("/recent", fnToHandler1 { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler { buffer: Buffer =>
        if(req.params.get("followed-teams") == null)
          throw IllegalHttpReqParams("Request param \"followed-teams\" expected ")

        val teams = req.params.get("followed-teams")
        val followedTeams = List(teams.split(","): _*).map({ line => line.substring(line.indexOf('=') + 1, line.length)})
        val responseWriter = new ChunkedResponseWriter(req, new AtomicInteger(followedTeams.size))

        val r = followedTeams.map({ teamName =>
          rxEventBus.send[JsonObject, JsonObject](pModule, recentResultByTeam(teamName))
        }) .map ({ ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.flatMap({ mes: RxMessage[JsonObject] =>
            val gamesId = ResponseFieldParserToArray(mes, "games_id")
            if (gamesId.isDefined) { rxEventBus.send[JsonObject, JsonObject](pModule, recentResultsById(gamesId.get)) }
            else { req.response.end; throw DBAccessException.create(s"Response parse error [RECENT GAMES ID] : ${mes.body}") }
          })
        }) .map { ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.subscribe( { mes: RxMessage[JsonObject] =>
            val responseChunk = ResponseParser(mes)
            if (responseChunk.isEmpty) {
              req.response.end; DBAccessException.create(s"Response parse error [RECENT GAMES] : ${mes.body}")
            } else {
              responseWriter.write(responseChunk.get)
            }
          },
          { th: Throwable => logger.info(th.getMessage) }
          )
        }
      }
    })

    router.getWithRegEx(".*", { req: HttpServerRequest =>
        req.path match {
          case "/" => req.response.sendFile(SportPlanetService.WEBROOT + SportPlanetService.LOGIN_PAGE)
          case r => req.response.sendFile(SportPlanetService.WEBROOT + URLDecoder.decode(req.path,"UTF-8"))
        }
    })

    import exts.fnToHandler1
    router.post("/center", fnToHandler1 { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler({ buffer: Buffer =>
        if (!buffer.getString(0, buffer.length).matches("email=[\\w]+&password+=[\\w]+"))
          req.response.sendFile(WEBROOT + LOGIN_FAILED_PAGE)
        val email = req.formAttributes.get(USER_EMAIL)
        val passwordHash = hash(req.formAttributes.get(USER_PASSWORD))

        import exts.{ fnToAction1 }
        val t = rxEventBus.send[JsonObject, JsonObject](pModule, createUserQuery(passwordHash, email))
        .subscribe({ mes: RxMessage[JsonObject] =>
          val followedTeams = ResponseFieldParser(mes, "followedTeams")
          req.response.headers.add("Set-Cookie", "auth-user=" + email)
          req.response.headers.add("Set-Cookie", "followed-teams=" +
            URLEncoder.encode(followedTeams getOrElse(
              throw DBAccessException create(s"Response parse error [USER-followedTeams] : ${mes.body}")), "UTF-8"))

          req.response.sendFile(WEBROOT + REPORTS_PAGE)
        }, {
          th: Throwable => logger.info(th.getMessage)
            req.response.sendFile(WEBROOT + LOGIN_FAILED_PAGE)
        })
      })
    }
    )

    server requestHandler(router)

    server listen(port)
  }
}