// server.js

// set up ======================================================================
// get all the tools we need
var express  = require('express');
var app      = express();
var port     = process.env.PORT || 9000;
var mongoose = require('mongoose');
var request  = require('request');
var fs = require('fs');

var cookieParser = require('cookie-parser');
var bodyParser   = require('body-parser');
var session      = require('express-session');

var configDB = require('./config/database.js');

var dataLocation = configDB.dataFolder;


// configuration ===============================================================
mongoose.connect(configDB.db); // connect to our database

app.all('*',function(req, res, next) {
    res.header('Access-Control-Allow-Methods','GET,PUT,POST,DELETE,OPTIONS');
    res.header('Access-Control-Allow-Origin', "*");
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Credentials', 'true');
    if (req.method == 'OPTIONS') {
        res.status(200).end();
    } else {
        next();
    }
    //next();
});

// require('./config/passport')(passport); // pass passport for configuration

// set up our express application
app.use(bodyParser()); // get information from html forms
//app.use(express.static(__dirname + '/app/views'));
app.set('view engine', 'ejs'); // set up ejs for templating

//


// routes ======================================================================
require('./app/parser/populateDb.js')(dataLocation);
//require('./app/utils/monitor.js')(dataLocation);
require('./app/routes/queries.js')(app);
/*require('./app/routes/flockEvents.js')(app, request);
require('./app/routes/stocks.js')(app,mongoose);*/

// launch ======================================================================
app.listen(port);
console.log('The magic happens on port ' + port);

app.get('/', function(req, res) {
    console.log('Success');
    res.json({success: "true", message: "Yayy"});
});




/*
function startReading(dir){
    var files = fs.readdirSync(dir);
    for(var i = 0 ; i< files.length ; i ++ ){
        if(!files[i].match(/\.*csv$/)){
            parseFileAndPopulateDb(files[i]) ;
        }
    }

};



startReading(dataLocation);*/
