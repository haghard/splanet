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
import scala.util.Try
import org.vertx.java.core.json.{JsonArray, JsonObject}
import io.vertx.rxcore.java.eventbus.{RxMessage, RxEventBus}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import org.vertx.java.core.http.{HttpServerRequest, RouteMatcher, HttpServer}
import com.google.common.io.BaseEncoding
import scala.Predef._
import scala.util.Failure
import scala.util.Success
import org.joda.time.DateTime

object SportPlanetService {

  val USER_EMAIL = "email"

  val USER_PASSWORD = "password"

  val LOGIN_PAGE = "/examples/sportPlanet/login.html"

  val REPORTS_PAGE = "/examples/sportPlanet/center.html"

  val LOGIN_FAILED_PAGE = "/examples/sportPlanet/login_failed.html";

  val WEBROOT = new File(".").getAbsolutePath.replace(".", "") + "web/bootstrap"

  val wins = List("homeWin", "awayWin")
  val lose = List("homeLose", "awayLose")

  //api access errors
  val error0 = "Bad credential in request"
  val error1 = "Unauthorized access"


  val RECENT_STAT_API = "/recent-stat/:team"
  val CONF_API = "/conference"
  val STANDING_API = "/standing/:flag"

  val STANDING2_API = "/standing2"

  val RECENT_TEAM_API = "/recent/:followedTeam"
  val RESULT_API = "/results/:day"

  def hash(password: String) =  Hashing.md5.newHasher.putString(password, Charsets.UTF_8).hash.toString
}

import SportPlanetService._
import com.gateway.server.exts.MongoPersistorKey

class SportPlanetService(implicit val bindingModule: BindingModule) extends Injectable {
  val logger = inject [Logger]
  val server = inject [HttpServer]
  val rxEventBus = inject [RxEventBus]

  val pModule = inject [String](MongoPersistorKey)
  val port = inject [Int](HttpServerPort)

  def start = {
    val router = new RouteMatcher

    /**
     * Rest resource /recent-stat/:team
     *
     **/
    import exts.{ fnToFunc1, fnToHandler1 }
    router get(RECENT_STAT_API, { req: HttpServerRequest =>


      req bodyHandler { buffer: Buffer =>
        Try(URLDecoder.decode(req.params.get("team"), "UTF-8")).map({ teamName =>
          rxEventBus.send(pModule, recentStat(teamName, 10)).subscribe({ mes: RxMessage[JsonObject] =>
            RecentHealthProcessor(mes, teamName)
              .fold(req.response.end(new JsonObject().putString("status", "empty").toString))({ js: JsonObject =>
                  req.response.end(js.toString) })
          }, { ex: Throwable =>
            logger.error(ex.getMessage)
            req.response.end(new JsonObject().putString("status", ex.getMessage).toString)
          })
        }).recover {
          case ex: Exception => {
            logger.error(ex.getMessage);
            req.response.end(new JsonObject().putString("status", ex.getMessage).toString)
          }
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
        { p:(String, String) => rxEventBus.send[JsonObject, JsonObject](pModule, userByEmail(p._1))
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

    router get(CONF_API, { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>
        rxEventBus.send[JsonObject, JsonObject](pModule, conferenceQuery).subscribe({ mes: RxMessage[JsonObject] =>
          Try(mes.body().getArray(MONGO_RESULT_FIELD)) match {
            case Success(array) => if (array.size() == 2) req.response().end(array.toString)
              else req.response.end(
                new JsonObject().putString("status", "error").putString("message","2 conference expected").toString)
            case Failure(ex) => req.response.end(
                new JsonObject().putString("status", "error").putString("message", ex.getMessage).toString)
          }
        }, {
          th: Throwable => logger.info(th.getMessage)
            req.response.end(
              new JsonObject().putString("status","error")
                .putString("ex", th.getMessage)
                .toString)
        })
      }
    })
    /**
     *  Rest resource /standing/:flag
     *  http://192.168.0.143:9000/standing/wins
     *  http://192.168.0.143:9000/standing/lose
     **/
    router get(STANDING_API, { req: HttpServerRequest =>
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
    router get(STANDING2_API, { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        def request(col: List[String]): rx.Observable[JsonObject] = {
          rx.Observable.merge(col.map { collectionName =>
            rxEventBus.send[JsonObject, JsonObject](pModule, standingQuery(collectionName))
          } .asJava).reduce(new JsonObject().putArray("results", new JsonArray()), reduceByMetrics)
        }
        request(wins ::: lose).subscribe({ json: JsonObject => req.response.end(json.toString) },
        { th: Throwable => logger.info(th.getMessage)
          req.response.end(new JsonObject().putString("status","error")
            .putString("body", th.getMessage).toString)
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
    router.get(RECENT_TEAM_API, { req: HttpServerRequest =>
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
          }, { th: Throwable =>
            logger.info(th.getMessage)
            req.response.end(new JsonObject().putString("status","error")
              .putString("body", th.getMessage).toString)
          })
        }
      }
    })

    /**
     *
     * @param r
     * @param finder
     * @return
     */
    def access(r: HttpServerRequest, finder:(String => rx.Observable[JsonObject])): rx.Observable[JsonObject] = {
      readBasicAuth(r).fold({ failure => rx.Observable.from(new JsonObject().putString("status", error0)) },
      { credential:(String, String) =>  rxEventBus.send[JsonObject, JsonObject](pModule, userByEmail(credential._1))
        .flatMap({ message: RxMessage[JsonObject] => Try(message.body().getArray("results"))
          .map({ arr => logger.info(s"${arr.get(0).asInstanceOf[JsonObject].getString("email")} access to /results/:day")
            rx.Observable.from(r.params.get("day"))
          }).getOrElse { logger.info(s"anon access to /results/:day"); rx.Observable.from("unknown") }
        }).flatMap(finder)
      })
    }

    /**
     *
     */
    router get(RESULT_API, { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>

        val resultPageFinder: (String) => rx.Observable[JsonObject] = { day =>
          day match {
            case "unknown" => rx.Observable.from(new JsonObject().putString("status", error1))
            case date => {
              val dt = QMongo.dateFormatter.parse(day)
              rxEventBus.send[JsonObject, JsonObject](pModule,
                periodResult(new DateTime(dt).minusDays(1).toDate, dt))
                .map { message: RxMessage[JsonObject] => message.body }
            }
          }
        }

        access(req, resultPageFinder).subscribe(
          { message: JsonObject => req.response.end(message.toString) },
          { th: Throwable => logger.info(th.getMessage); req.response.end(th.getMessage) }
        )

      }})

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
    /*router get("/recent", { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler { buffer: Buffer =>
        if(Option(req.params.get("followed-teams")).isEmpty)
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
          { th: Throwable => logger.info(th.getMessage)
            req.response.end(new JsonObject().putString("status","error")
              .putString("body", th.getMessage).toString)
          })
        }
      }
    })*/

    router.getWithRegEx(".*", { req: HttpServerRequest =>
        req.path match {
          case "/" => req.response.sendFile(SportPlanetService.WEBROOT + SportPlanetService.LOGIN_PAGE)
          case r => req.response.sendFile(SportPlanetService.WEBROOT + URLDecoder.decode(req.path,"UTF-8"))
        }
    })

    import exts.fnToHandler1
    router.post("/center", { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler({ buffer: Buffer =>
        if (!buffer.getString(0, buffer.length).matches("email=[\\w+@[a-zA-Z]+?\\.[a-zA-Z]{2,6}]+&password+=[\\w]+"))
          req.response.sendFile(WEBROOT + LOGIN_FAILED_PAGE)
        val email = req.formAttributes.get(USER_EMAIL)
        val passwordHash = hash(req.formAttributes.get(USER_PASSWORD))

        import exts.{ fnToAction1 }
        val t = rxEventBus.send[JsonObject, JsonObject](pModule, userQuery(passwordHash, email))
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
    })

    server requestHandler(router)

    logger.info("Http server started")

    server listen(port)
  }

  def readBasicAuth(req: HttpServerRequest): Either[String, (String, String)] = {
    val BasicHeader = "Basic (.*)".r
    Option(req.headers.get("Authorization")).map { v => v match {
        case BasicHeader(base64) => {
          try {
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