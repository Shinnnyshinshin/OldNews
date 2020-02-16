
// set up of packages
const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const assert = require('assert')
const access = require('./scripts/database_access.js')
const parsing = require('./scripts/parsing.js')
const params = require('./scripts/params.js')
// set up app
const app = express();
app.use(express.static('public'))
app.use(bodyParser.urlencoded({ extended: true }));
app.set('view engine', 'ejs')

// connecting to the DBs (TODO : add repository as env?)
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var mongoUrl = params.monogoURL;

var redisClient = require('redis').createClient;
var redis = redisClient(params.redisport, params.redisURL);

// global keyword
var current_keyword = "";


// initial index
app.get('/', function (req, res) {
    res.render('index.ejs');
});

// initial search
app.post('/', function(req, res) {
  var start = new Date(); // for cim
  if (req.body == '') res.status(400).send("Please send a proper keyword");
  MongoClient.connect(mongoUrl, function (err, db) {
      if (err) throw 'Error connecting to database - ' + err;
      else{
         // extract keyword
	       current_keyword = req.body.keyword;
	          access.findKeywordWithCache(db, redis, req.body.keyword, function(keywords, cache_or_redis){
              if (!keywords) {
                res.render('tryagain',{keyword:current_keyword, error:null})
              }else{
          var ordered = parsing.ordertimestamps(keywords)
           var returnvalues = parsing.calculate_x_y_values(ordered, keywords)
           // storing
           var howlong =  new Date() - start;
           res.render('result_short_cache', {xval: returnvalues[0], yval: returnvalues[1], howlong: howlong, ratio:returnvalues[2], upordown:returnvalues[3],  keyword:current_keyword, error: null});
	      }})}
       })
        });

app.get('/update_plot_short', (req, res, next) => {
  var start = new Date();
   try {
      MongoClient.connect(mongoUrl, function(err, client) {
       assert.equal(null, err);
       const db = client.db('all_tweets');
       //Step 1: declare promise to handle asynchronous queries
       var myPromise = () => {
         return new Promise((resolve, reject) => {
            db
             .collection('tweets_uncompressed')
             .find({"Keyword": { $regex: current_keyword.concat('-----')}})
             .toArray(function(err, data) {
                 err
                    ? reject(err)
                    : resolve(data);
               });
         });
       };

       //Step 2: async promise handler
       var callMyPromise = async () => {
          var result = await (myPromise());
          //anything here is executed after result is resolved
          return result;
       };

       //Step 3: make the call
       callMyPromise().then(function(result) {
          client.close();
          //console.log(result)
          var keywords = new Object();
          var newkey = ""
          result.forEach(function(element) {
            newkey = element['Keyword'].split("-----")[1];
            var times = Object.keys(element['Time'])
            times.forEach(function(time){
              keywords[newkey.concat(' ', time)] = element['Time'][time]
            })
          }
          )
           var ordered = parsing.ordertimestamps(keywords)
           var returnvalues = parsing.calculate_x_y_values(ordered, keywords)
           // storing
           var howlong =  new Date() - start;
           res.render('short_term_db', {xval: returnvalues[0], yval: returnvalues[1], howlong: howlong, ratio:returnvalues[2], upordown:returnvalues[3],  keyword:current_keyword, error: null});
           return(res);
          // so I need to get the result and build the function
          //res.render('result', {xval: xval, yval: yval, error: null});
          //res.json(result);
      });
    }); //end mongo client
   } catch (e) {
     next(e)
   }
});

app.get('/update_plot_all', function(req, res) {
  var start = new Date();

  if (req.body == '') res.status(400).send("Please send a proper keyword");
  MongoClient.connect(mongoUrl, function (err, db) {
      if (err) throw 'Error connecting to database - ' + err;
      else{

	          access.findKeywordWithMongoOnly_longterm(db, current_keyword, function(keywords, cache_or_redis){
              if (!keywords) res.status(500).send('Could not find in Database')
              else{

       var ordered = parsing.ordertimestamps(keywords)
        var returnvalues = parsing.calculate_x_y_values(ordered, keywords)
        var howlong =  new Date() - start;
       res.render('full_db', {xval: returnvalues[0], yval: returnvalues[1], howlong: howlong, ratio:returnvalues[2], upordown:returnvalues[3],  keyword:current_keyword, error: null});
       console.log('Long Mongo Request took:', new Date() - start, 'ms');
       return(res);
                }
            })
        }})});



app.listen(3000, function () {
  console.log('OldNews app listening on port 3000!')
});
