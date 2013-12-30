var container = require('vertx/container');

var console = require('vertx/console');

container.deployModule('io.vertx~mod-mongo-persistor~2.1.0', function(err, deployID) {

    // And when it's deployed run a script to load it with some reference
    // data for the demo
    if (!err) {
        console.error("loaded");
        //load('static_data.js');
    } else {
        console.error("error");
        err.printStackTrace();
    }
});