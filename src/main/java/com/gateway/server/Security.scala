package com.gateway.server

import com.gateway.server.QMongo._
import org.vertx.java.core.http.HttpServerRequest
import org.vertx.java.core.json.JsonObject
import io.vertx.rxcore.java.eventbus.RxMessage
import scala.util.Try
import com.google.common.io.BaseEncoding
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable
import com.gateway.server.exts.Principal

/**
 * Created by haghard on 27/04/14.
 */
trait Security extends SportPlanetService {

  private val BasicHttpHeader = "Basic (.*)".r

  private val secError0 = "invalid/absent credential in request"
  private val secError1 = "user not found in db"
  private val secError2 = "input is not a valid encoded string according to this encoding"
  private val secError3 = "wrong authorization type"
  private val secError4 = "Authorization header not found"

  private def emptyObs() : Observable[Either[String, Principal]] = Observable.items(Left(secError4))
  private val emptyPrincipal : Either[String, Principal] = Left(secError1)

  /**
   *
   * @param r
   * @return
   */
  override def lookupPrincipal(r: HttpServerRequest): rx.lang.scala.Observable[Either[String, Principal]] = {
    extractToken(r) match {
      case Some(eit) => {
        val principal = eit.right.get
        toScalaObservable(
          rxEventBus.send[JsonObject, JsonObject](pModule, userByEmail(principal.email))
        ).map({ mes: RxMessage[JsonObject] =>
          Try {
            val res = mes.body.getArray("results").get(0).asInstanceOf[JsonObject].getString("email")
            if (res == null) {
              throw new IllegalArgumentException(secError1)
            }

            Right(Principal(principal.email,"XXX"))
          } getOrElse (emptyPrincipal)
        })

      }
      case None => emptyObs()
    }
  }

  private def extractToken(req: HttpServerRequest): Option[Either[String, Principal]] =
    Option(req.headers.get("Authorization")).map({ v =>
      v match {
        case BasicHttpHeader(base64) =>
          Try({
            new String(BaseEncoding.base64().decode(base64)).split(":", 2) match {
              case Array(username, password) => Right(Principal(username, password))
              case _ => Left(secError0)
            }
          }).recover ({
            case err: Exception => Left(secError2)
          }).get

        case _ => Left(secError3)
      }
    })
}