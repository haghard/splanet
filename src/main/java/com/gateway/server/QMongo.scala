package com.gateway.server

import java.util.Date
import java.text.SimpleDateFormat
import org.vertx.java.core.json.JsonObject
import org.vertx.java.core.json.JsonArray

/**
 * db.results.ensureIndex({dt:1,homeTeam:1, awayTeam:1})
 * db.settings.ensureIndex({ startDt:1, endDt:1 })
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

  //db.settings.ensureIndex({ startDt:1, endDt:1 })
  def currentStage(currentDt: Date) = new JsonObject()
      .putString("collection", "settings")
      .putString("action", "find")
      .putObject("matcher", new JsonObject().putString("query",
      " $and: [ { startDt $lt \"ISODate=" + format(currentDt) + "\" }, { endDt $gt \"ISODate=" + format(currentDt) + "\" }] "))

  def playoffResults(stageName: String, location: String) = new JsonObject()
    .putString("collection", s"${location}-${stageName}")
    .putString("action", "find")
    .putObject("sort", new JsonObject().putNumber("dt", -1))
    .putNumber("limit", 30)


  def resultWindow(startDt: Date, endDt: Date) = new JsonObject()
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