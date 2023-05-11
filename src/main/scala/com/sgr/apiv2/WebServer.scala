package com.sgr.apiv2

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.{Directive, Directive0, Directive1, HttpApp, Route}

import java.util.function.Supplier


/**
 * @author sunguorui
 * @date 2022年07月05日 5:38 下午
 */
case class User(id: Option[Int], username: String, age: Int)

class WebServer extends HttpApp {
  import JacksonSupport._
    override def routes(): Route = {
//      path("hello", new Supplier[Route] {
//          override def get(): Route = {
//          println("abc")
//          complete("Say hello to akka-http")
//        }
//      })
      topLevelRoute
    }
//  val route1: Route =
//    pathPrefix("tpcds") {
//      pathEndOrSingleSlash { // POST /user
//        post {
//          entity(as[User]) { payload =>
//            complete(payload)
//          }
//        }
//      } ~
//        pathPrefix(IntNumber) { userId =>
//          get { // GET /user/{userId}
//            complete(User(Some(userId), "", 0))
//          } ~
//            put { // PUT /user/{userId}
//              entity(as[User]) { payload =>
//                complete(payload)
//              }
//            } ~
//            delete { // DELETE /user/{userId}
//              complete("Deleted")
//            }
//        }
//    }
    val route: Route =
      pathPrefix("tpcds") {
        pathEndOrSingleSlash {
          get {
            complete("全流程测试")
          }
        } ~
          pathPrefix(IntNumber) { userId =>
            get {
              complete(userId)
            }
          }
      }
  val route2: Route = {
    get {
      complete("全流程测试")
    }
  }

  val route3: Route = {
    pathPrefix(IntNumber) { scale =>
      get {
        complete(scale)
      }
    }
  }

  val route4: Route = {
    get {
      complete("sql测试")
    }
  }

  val route5: Route = {
    pathEnd {
      get {
        complete("全流程测试")
      }
    } ~
      path(IntNumber) { userId =>
        get {
          complete(userId)
        }
      }
  }

  lazy val topLevelRoute: Route =
  // provide top-level path structure here but delegate functionality to subroutes for readability
    concat(
      pathPrefix("tpcds")(route5),
      // extract URI path element as Int
      path("sqlTest")(route4),
      pathPrefix("genData")(route3),
    )
}
