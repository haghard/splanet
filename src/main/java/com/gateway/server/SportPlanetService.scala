package com.gateway.server

import QMongo._
import java.io.File
import org.vertx.java.core.logging.Logger
import org.vertx.java.core.buffer.Buffer
import com.google.common.hash.Hashing
import com.google.common.base.Charsets
import java.util.concurrent.atomic.AtomicInteger
import com.gateway.server.exts._
import java.net.{URLEncoder, URLDecoder}
import scala.collection.JavaConverters._
import scala.util.{Failure, Try, Success}
import com.gateway.server.exts.IllegalHttpReqParams
import org.vertx.java.core.json.{JsonArray, JsonObject}
import io.vertx.rxcore.java.eventbus.{RxMessage, RxEventBus}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import org.vertx.java.core.http.{HttpServerRequest, RouteMatcher, HttpServer}
import com.google.common.io.BaseEncoding
import scala.Predef._
import scala.util.Failure
import scala.Some
import scala.util.Success
import com.gateway.server.exts.IllegalHttpReqParams

object SportPlanetService {

  val USER_EMAIL = "email"

  val USER_PASSWORD = "password"

  val LOGIN_PAGE = "/examples/sportPlanet/login.html"

  val REPORTS_PAGE = "/examples/sportPlanet/center.html"

  val LOGIN_FAILED_PAGE = "/examples/sportPlanet/login_failed.html";

  val WEBROOT = new File(".").getAbsolutePath.replace(".", "") + "web/bootstrap"

  val wins = List("homeWin", "awayWin")
  val lose = List("homeLose", "awayLose")

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

    /**
     * Basic http auth
     */
    router get("/auth", { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        readBasicAuth(req) fold(
        { failure => req.response.end(new JsonObject().putString("status","error").toString) },
        { p:(String, String) => rxEventBus.send[JsonObject, JsonObject](pModule, createUserQuery(hash(p._2), p._1))
            .subscribe({ mes: RxMessage[JsonObject] =>
              val followedTeams = ResponseFieldParser(mes, "followedTeams")
              val ft = URLEncoder.encode(followedTeams getOrElse(
                throw DBAccessException create(s"Response parse error [USER-followedTeams] : ${mes.body}")), "UTF-8")

              req.response.end(new JsonObject().putString("status","ok").putString("followed-teams", ft).toString)
          }, {
            th: Throwable => logger.info(th.getMessage)
              req.response.end(new JsonObject().putString("status","error").toString)
          })
        })
    }})

    router get("/conference", { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>
        rxEventBus.send[JsonObject, JsonObject](pModule, conferenceQuery).subscribe({ mes: RxMessage[JsonObject] =>
          for {
           array <- Some(mes.body().getArray(MONGO_RESULT_FIELD))
          } yield {
            if (array.size == 2) req.response().end(array.toString)
            else { req.response.end(new JsonObject()
              .putString("status", "error")
              .putString("message", "expected 2 conference")
              .toString) }
          }
        })
      }
    })
    /**
     *  Rest resource /standing/:flag
     *  http://192.168.0.143:9000/standing/wins
     *  http://192.168.0.143:9000/standing/lose
     **/
    router get("/standing/:flag", { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        val flag = req.params.get("flag")

        def request(col: List[String]): rx.Observable[JsonObject] = {
          rx.Observable.merge(col.map { collectionName =>
            rxEventBus.send[JsonObject, JsonObject](pModule, standingQuery(collectionName))
          } .asJava)
            .reduce(new JsonObject().putArray("results", new JsonArray()), reduceAll)
        }

        val ob: Try[rx.Observable[JsonObject]] = flag match {
          case "wins" => Success(request(wins))
          case "lose" => Success(request(lose))
          case e => Failure(new IllegalArgumentException("unexpected param in request " + e))
        }

        ob match {
          case Success(ob) => ob.subscribe { message : JsonObject => req.response().end(message.toString) }
          case Failure(er) => { logger.info(er.getMessage);
            req.response.end(new JsonObject()
              .putString("status","error")
              .putString("message", er.getMessage)
              .toString)
          }
        }
      }
    })

    /**
     *
     */
    router get("/standing2", { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        def request(col: List[String]): rx.Observable[JsonObject] = {
          rx.Observable.merge(col.map { collectionName =>
            rxEventBus.send[JsonObject, JsonObject](pModule, standingQuery(collectionName))
          } .asJava).reduce(new JsonObject().putArray("results", new JsonArray()), reduceByMetrics)
        }
        request(wins ::: lose).subscribe { json: JsonObject =>
          req.response.end(json.toString)
        }
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
    router.get("/recent/:followedTeam", { req: HttpServerRequest =>
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

    router get("/tops", { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>
        rxEventBus.send(pModule, topResults(10)).subscribe({ message: RxMessage[JsonObject] =>
          req.response.end(message.body.toString)
        },{
          th: Throwable => logger.info(th.getMessage)
        })
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
    router.get("/recent", { req: HttpServerRequest =>
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

  def readBasicAuth(req: HttpServerRequest): Either[String, (String, String)] = {
    val BasicHeader = "Basic (.*)".r
    Option(req.headers.get("Authorization")).map { v => v match {
        case BasicHeader(base64) => {
          try {
            //import org.apache.commons.codec.binary.Base64
            //new String(Base64.decodeBase64(base64))
            new String(BaseEncoding.base64().decode(base64)).split(":", 2) match {
              case Array(username, password) => Right(username -> password)
              case _ => Left("Invalid basic auth 1")
            }
          } catch {
            case err => Left("Invalid basic auth 2")
          }
        }
        case _ => Left("Bad Auth header")
        }
    } getOrElse(Left("Auth type absent in header"))
  }
}