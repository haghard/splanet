package com.gateway.server

import java.util.concurrent.{TimeUnit, ExecutorService}
import scala.concurrent.Future
import com.ning.http.client.{AsyncHttpClientConfig, AsyncHttpClient}
import scala.util.{Failure, Success, Try}
/**
 *
 */
object IO {

  trait WebClient {
    def get(url: String)(exec: ExecutorService): Future[String]
    def close(): Unit
  }

  case class BadStatusException(body: String, status: Int) extends RuntimeException(body)

  class AsyncWebClient extends WebClient {
    val client = new AsyncHttpClient(
      new AsyncHttpClientConfig.Builder()
        .setConnectionTimeoutInMs(3000)
        .build()
    )

    override def get(url: String)(exec: ExecutorService): Future[String] = Try {
      val future = client.prepareGet(url).execute
      val p = scala.concurrent.Promise[String]()
      future.addListener(new Runnable {
        def run = {
          val response = Try(future.get(3000, TimeUnit.MILLISECONDS))
          response match {
            case Success(res) => {
              res.getStatusCode match {
                case 404 => p.success(res.getResponseBody)
                case other => p.failure(BadStatusException("error", 400))
              }
            }
            case Failure(ex) =>
              p.failure(BadStatusException(ex.getMessage, 400))
          }
        }
      }, exec)
      p.future
    }.recover {
      case e: Exception => throw BadStatusException(e.getMessage, 400)
    }.get

    override def close() = client.close()
  }
}