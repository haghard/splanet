(function (global, $, undefined) {

    function fetchRecent(team) {
        var promise = $.ajax({
            url: 'http://192.168.0.143:9000/recent/' + team
            //data: 'followed-teams=' + team
        }).promise();
        return Rx.Observable.fromPromise(promise);
    }

    function getCookieValue(cname) {
        var allCookies = document.cookie.split(';');
        for (var i = 0; i < allCookies.length; i++) {
            var current = allCookies[i].trim();
            if (current.indexOf(cname) == 0) {
                return current.replace(cname + "=", "");
            }
        }
        return "Unknown";
    }

    function RecentResult(json) {
        var that = this;
        that._id = json._id;
        that.dt = new Date(json.dt);
        that.homeTeam = json.homeTeam;
        that.awayTeam = json.awayTeam;
        that.homeScore = json.homeScore;
        that.awayScore = json.awayScore;
        that.homeTeamImage = "images/" + urlEncode(json.homeTeam) + ".gif";
        that.awayTeamImage = "images/" + urlEncode(json.awayTeam) + ".gif";
    }

    function urlEncode(str) {
        return encodeURIComponent((str+'').replace(/\+/g, '%20'));
    }

    function main() {
        //var singletonArray = [ getCookieValue("followed-teams") ];

        //now we do n call each one for team
        var singletonArray = getCookieValue("followed-teams").split(urlEncode(","))

        Rx.Observable.fromArray(singletonArray).flatMap(
           function(team) {
              return fetchRecent(team)
              //return fetchRecent(getCookieValue("followed-teams"))
           }
        ).subscribe( function(data) {
          var json = JSON.parse(data);
          var resultArray = [];
          var winCount = 0;
          var loseCount = 0;

          for (var i = 0; i < json.results.length; i++) {
             resultArray[i] = new RecentResult(json.results[i]);
          }

          $.each(resultArray, function () {
             var item = ' <div class="col-sm-4" style="width: 320px; height: 110px;"> ' +
               ' <div class="panel panel-default"> ' +
               ' <div class="panel-heading"> ' +
               ' ' +
               ' <b>' + this.dt.toString().substring(0, 16) + ' </b> ' +
               ' <h3 class="panel-title">' +
               ' <input type="image" style="width: 75px; height: 50px; no-repeat 0 0; background-position:0 -40px; border: 0pt none; margin: 0px 0px;" src='+ this.homeTeamImage +'>' +
               ' <label> ' + this.homeScore +' </label> ' +
               ' <label> ' + ' &nbsp &nbsp &nbsp ' +' </label> ' +
               ' <label> '+ this.awayScore +' </label> ' +
               ' <input type="image" style="width: 75px; height: 50px; no-repeat 0 0; background-position:0 -40px; border: 0pt none; margin: 0px 0px;" src='+ this.awayTeamImage +'>'
           '</div> </div>'

           $("#RecentResults").append(item);
          })


           /*for (var i = 0; i < json.results.length; i++) {
               if (resultArray[i].homeScore > resultArray[i].awayScore) {

               } else {

               }
           }

          $("#FollowedTeams").append(' <label> ' + winCount + '-' + loseCount + ' </label> ');*/
        },
        function (error) {
            alert("Json parse error" + error);
        });

        /*
        var $input = $('#textInput'), $results = $('#results');
        // Get all distinct key up events from the input and only fire if long enough and distinct
        var keyup = Rx.Observable.fromEvent($input, 'keyup')
                .map(function (e) {
                    return e.target.value; // Project the text from the input
                })
                .filter(function (text) {
                    return text.length > 2; // Only if the text is longer than 2 characters
                })
                .throttle(750 *//* Pause for 750ms *//* )
                .distinctUntilChanged(); // Only if the value has changed
        var searcher = keyup.flatMapLatest(
                function (text) {
                    return searchWikipedia(text); // Search wikipedia
                });
        var subscription = searcher.subscribe(
                function (data) {
                    var res = data[1];

                    // Append the results
                    $results.empty();

                    $.each(res, function (_, value) {
                        $('<li>' + value + '</li>').appendTo(results);
                    });
                },
                function (error) {
                    // Handle any errors
                    $results.empty();

                    $('<li>Error: ' + error + '</li>').appendTo(results);
                });
                */
    }

    main();


}(window, jQuery));