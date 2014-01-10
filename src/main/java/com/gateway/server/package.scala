package com.gateway.server

import org.vertx.java.core.http.{HttpServerRequest, HttpServerResponse}
import org.vertx.java.core.Handler
import rx.util.functions.{Func1, Action1}
import java.util.concurrent.atomic.AtomicInteger

package object exts {

  implicit def fnToHandler[T](f: T => HttpServerResponse): Handler[T] = new Handler[T]() {
    override def handle(req: T) = f(req)
  }

  implicit def fnToHandler1[T](f: T => Unit): Handler[T] = new Handler[T]() {
    override def handle(req: T) = f(req)
  }

  implicit def fnToAction1[T](fn: T => HttpServerResponse): Action1[T] = new Action1[T]() {
    override def call(event: T) = fn(event)
  }

  implicit def fnToAction2[T](fn: T => Unit): Action1[T] = new Action1[T]() {
    override def call(event: T) = fn(event)
  }

  implicit def fnToAction3[T](fn: T => Any): Action1[T] = new Action1[T]() {
    override def call(event: T) = fn(event)
  }

  implicit def fnToFunc1[T, E](fn: T => E): Func1[T, E] = new Func1[T, E]() {
    override def call(event: T) = fn(event)
  }

  trait ResponseWriter {
    def write(line: String)
  }

  class ChunkedResponseWriter(req: HttpServerRequest, threshold: AtomicInteger,
                              currentChuckNumber: AtomicInteger = new AtomicInteger) extends ResponseWriter {

    def write(line: String): Unit = currentChuckNumber.incrementAndGet() match {
      case n => {
        if (n == 1) req.response().write("{ \"results\": [" + line)
        else if (n > 1) req.response().write("," + line)

        if( n == threshold.get()) { req.response().end("] }") }
      }
    }
  }
}
