package com.gateway.server

import org.vertx.java.core.json.{JsonArray, JsonObject}
import java.util.Date
import java.text.SimpleDateFormat

/**
 *
 * Standing with mongo aggregation framework
 *
 * home win
 * db.results.aggregate([{ $group: { _id: "$homeTeam",  wins: { $sum: {  $cond : [ { $gt: [ "$homeScore", "$awayScore" ] }, 1, 0 ]}} } } ] )
 * away win
 * db.results.aggregate([{ $group: { _id: "$awayTeam",  wins: { $sum: {  $cond : [ { $gt: [ "$awayScore", "$homeScore" ] }, 1, 0 ]}} } } ] )
 *
 * home lose
 * db.results.aggregate([{ $group: { _id: "$homeTeam",  wins: { $sum: {  $cond : [ { $gt: [ "$awayScore", "$homeScore" ] }, 1, 0 ]}} } } ] )
 * away lose
 * db.results.aggregate([{ $group: { _id: "$awayTeam",  wins: { $sum: {  $cond : [ { $gt: [ "$homeScore", "$awayScore" ] }, 1, 0 ]}} } } ] )
 *
 */
object QMongo {

  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")

  def format(dt: Date) = dateFormatter.format(dt)

  def periodResult(startDt: Date, endDt: Date) = new JsonObject()
    .putString("collection", "results")
    .putString("action", "find")
    .putObject("matcher", new JsonObject().putString("query",
      " $and: [ { dt $gt \"ISODate=" + format(startDt) + "\" }, { dt $lt \"ISODate=" + format(endDt) + "\"} ] "))

  def topResults(limit: Int) = new JsonObject()
    .putString("collection", "results")
    .putString("action", "find")
    .putObject("sort", new JsonObject().putNumber("dt", -1))
    .putNumber("limit", limit)


  def conferenceQuery = new JsonObject()
    .putString("collection", "conference")
    .putString("action", "find")


  /**
   * homeWinMap
   * awayWinMap
   * homeLoseMap
   * awayLoseMap
   *
   * @param collectionName
   * @return
   */
  def standingQuery(collectionName: String) = new JsonObject()
    .putString("collection", collectionName)
    .putString("action", "find")
    .putNumber("batch_size", 30)
    .putObject("sort", new JsonObject().putNumber("_id", -1))

  def userByEmail(email: String) = new JsonObject()
    .putString("collection", "users")
    .putString("action", "find")
    .putObject("matcher",
      new JsonObject().putString("email", email))


  def userQuery(passwordHash: String, email: String) = new JsonObject()
    .putString("collection", "users")
    .putString("action", "find")
    .putObject("matcher", new JsonObject()
    .putString("query", " email $eq \"" + email + "\"" + " password $eq \"" + passwordHash + "\""))

  /*def followedTeams(ids: java.util.Iterator[Object]): JsonObject = {
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
  */

  //db.results.ensureIndex({dt:1,homeTeam:1, awayTeam:1})
  def recentStat(teamName: String, limit: Int) = new JsonObject()
    .putString("collection", "results")
    .putString("action", "find")
    .putObject("sort", new JsonObject().putNumber("dt", -1))
    .putNumber("limit", limit)
    .putNumber("batch_size", limit)
    .putObject("matcher", new JsonObject()
    .putString("query", " $or : [ { homeTeam $eq \"" + teamName + "\"" + " }, { awayTeam $eq \"" + teamName + "\"" + " } ]" ))

  def recentResultByTeam(teamName: String) = new JsonObject()
    .putString("collection", "recent")
    .putString("action", "find")
    .putObject("matcher",
      new JsonObject().putString("name", teamName))

  def recentResultsById(recentIds: JsonArray) = {
    val resultLine = new scala.StringBuilder()
    val cleanLine = recentIds.toString.replaceAll("\\[\"|\\\"]", "")
    resultLine.append("\"").append(cleanLine).append("\"")

    new JsonObject().putString("collection", "results").putString("action", "find")
       .putObject("sort", new JsonObject putNumber ("dt", 1))
       .putObject("matcher", new JsonObject putString("query", "_id $in { " + resultLine.toString + " }"))
  }
}
