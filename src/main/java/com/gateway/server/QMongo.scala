package com.gateway.server

import org.vertx.java.core.json.{JsonArray, JsonObject}
import scala.StringBuilder

object QMongo {

  def topResults(limit: Int) = {
    new JsonObject()
      .putString("collection", "results")
      .putString("action", "find")
      .putObject("sort", new JsonObject().putNumber("dt", -1))
      .putNumber("limit", limit)
  }

  def conferenceQuery = {
    new JsonObject()
      .putString("collection", "conference")
      .putString("action", "find")
  }
  /**
   * homeWinMap
   * awayWinMap
   * homeLoseMap
   * awayLoseMap
   *
   * @param collectionName
   * @return
   */
  def standingQuery(collectionName: String): JsonObject = {
    new JsonObject().putString("collection", collectionName)
      .putString("action", "find")
      .putObject("sort", new JsonObject().putNumber("_id", -1))
  }

  def userByEmailQuery(email: String): JsonObject = {
    new JsonObject().putString("collection", "users")
      .putString("action", "find")
      .putObject("matcher",
        new JsonObject().putString("email", email))
  }

  def userQuery(passwordHash: String, email: String): JsonObject = {
    new JsonObject().putString("collection", "users")
      .putString("action", "find")
      .putObject("matcher", new JsonObject()
      .putString("query", " email $eq \"" + email + "\"" + " password $eq \"" + passwordHash + "\""))
  }

  def followedTeams(ids: java.util.Iterator[Object]): JsonObject = {
    import scala.collection.JavaConversions.asScalaIterator
    val scalaIds: Iterator[Object] = ids

    var first = true
    val resultsLine = new StringBuilder()

    for (id <- scalaIds) {
      if (first) {
        resultsLine.append("\"").append(id.toString).append("\"");
        first = false;
      } else {
        resultsLine.append(",").append("\"").append(id.toString).append("\"");
      }
    }

    new JsonObject().putString("collection", "teams")
      .putString("action", "find")
      .putObject("matcher", new JsonObject()
      .putString("query", " _id $in { " + resultsLine.toString() + " }"))
  }

  def recentStat(teamName: String, limit: Int): JsonObject = {
    new JsonObject().putString("collection", "results")
      .putString("action", "find")
      .putObject("sort", new JsonObject().putNumber("dt", -1))
      .putNumber("limit", limit)
      .putObject("matcher", new JsonObject()
        .putString("query", " $or : [ { homeTeam $eq \"" + teamName + "\"" + " }, { awayTeam $eq \"" + teamName + "\"" + " } ]" ))
  }

  def recentResultByTeam(teamName: String): JsonObject = {
    new JsonObject().putString("collection", "recent")
      .putString("action", "find")
      .putObject("matcher",
        new JsonObject().putString("name", teamName));
  }

  def recentResultsById(recentIds: JsonArray): JsonObject = {
    val resultLine = new scala.StringBuilder()
    val cleanLine = recentIds.toString.replaceAll("\\[\"|\\\"]", "")
    resultLine.append("\"").append(cleanLine).append("\"")

    new JsonObject().putString("collection", "results").putString("action", "find")
       .putObject("sort", new JsonObject putNumber ("dt", 1))
       .putObject("matcher", new JsonObject putString("query", "_id $in { " + resultLine.toString + " }"))
  }
}
