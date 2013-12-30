var vertx = require('vertx');
var console = require('vertx/console');

alert("1");
vertx.createHttpClient().port(9000).host('192.168.0.143').getNow('/recent', function (req) {
    console.log("Got response " + resp.statusCode());
    resp.bodyHandler(function (body) {
        console.log("Got data " + body);
    })
})


/*
 (function DemoViewModel() {

 var that = this;
 alert("DemoViewModel" + window.location.protocol + '//' + window.location.hostname + ':' + window.location.port + '/splanet/eventbus');
 var eb = new vertx.EventBus(window.location.protocol + '//' + window.location.hostname + ':' + window.location.port + '/splanet/eventbus');
 that.items = ko.observableArray([]);

 eb.onopen = function() {

 alert("onopen");

 eb.send('vertx.mongopersistor', {action: 'find', collection: 'recentNbaResults', matcher: {} },
 function(reply) {
 if (reply.status === 'ok') {
 var recentResultsNba = [];
 for (var i = 0; i < reply.results.length; i++) {
 recentResultsNba[i] = new RecentNbaResult(reply.results[i]);
 }
 that.albums = ko.observableArray(recentResultsNba);
 ko.applyBindings(that);
 } else {
 console.error('Failed to retrieve recent-results-nba: ' + reply.message);
 }
 });
 };

 eb.onclose = function() {
 eb = null;
 };

 function RecentNbaResult(json) {
 var that = this;
 that._id = json._id;
 that.genre = json.homeTeam;
 that.artist = json.awayTeam;
 that.title = json.homeScore;
 that.price = json.awayScore;
 }

 })();*/
