const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const app = express();
var assert = require('assert')

const http = require('http').Server(app);
const io = require('socket.io')(http);

var access = require('./access.js')

// connecting to the DBs ( should this be)
var mongodb = require('mongodb');
//var url = 'mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com:27017/pymongo_test';
var MongoClient = mongodb.MongoClient;
var mongoUrl = 'mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com:27017';

var redisClient = require('redis').createClient;
var redis = redisClient(6379, 'redis-clust.wdeoii.ng.0001.usw2.cache.amazonaws.com');

var current_keyword = "";

app.use(express.static('public'))
app.use(bodyParser.urlencoded({ extended: true }));
app.set('view engine', 'ejs')

app.get('/', function (req, res) {
    res.render('index.ejs');
});


app.get('/update_plot_short', (req, res, next) => {
  var start = new Date();

   try {
      MongoClient.connect(mongoUrl, function(err, client) {
       assert.equal(null, err);
       const db = client.db('all_tweets');

       //Step 1: declare promise

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
          var resultsTol_dict = new Object();
          var newkey = ""
          result.forEach(function(element) {
            newkey = element['Keyword'].split("-----")[1];
            var times = Object.keys(element['Time'])
            times.forEach(function(time){
              resultsTol_dict[newkey.concat(' ', time)] = element['Time'][time]
              //console.log(newkey.concat(' ', time))
              //console.log(element['Time'][time])
            })
          }
          )


          // now we go through the normal steps
          var ordered = Object.keys(resultsTol_dict)
         var newordered = ordered.sort(function compare(a,b){
         var datea = new Date(a)
         var dateb = new Date(b)
         return datea - dateb;
         });

         var xval = "";
         var yval = [];

   newordered.forEach(function(key) {
     xval = xval.concat(",\'", key, "\'");
     yval.push(resultsTol_dict[key]);
   });
      xval = xval.substring(1)

      // this is just the ratio
      var y_first = yval[1];
      var y_last = yval.slice(-1)[0];
      var ratio = y_last / y_first
      var upordown = "not changed"
      if  (ratio >1){
        upordown = "increased"
      }else{
        upordown = "decreased"
      }
      // formatting
      ratio = (ratio-1) * 100;
      ratio = ratio.toFixed(1)
      var howlong =  new Date() - start;

      res.render('result_short_mongo', {xval: xval, yval: yval, howlong: howlong, ratio:ratio, upordown:upordown,  keyword:current_keyword, error: null});

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
	        var ordered = Object.keys(keywords)
	        var newordered = ordered.sort(function compare(a,b){
			    var datea = new Date(a)
			    var dateb = new Date(b)
			    return datea - dateb;
		      });

		var xval = "";
		var yval = [];

		newordered.forEach(function(key) {
		  xval = xval.concat(",\'", key, "\'");
		  yval.push(keywords[key]);
		});
	     xval = xval.substring(1)

       // this is just the ratio
       var y_first = yval[1];
       var y_last = yval.slice(-1)[0];
       var ratio = y_last / y_first
       var upordown = "not changed"
       if  (ratio >1){
         upordown = "increased"
       }else{
         upordown = "decreased"
       }
       // formatting
       ratio = (ratio-1) * 100;
       ratio = ratio.toFixed(1)
       var howlong =  new Date() - start;

       res.render('result_all_mongo', {xval: xval, yval: yval, howlong: howlong, ratio:ratio, upordown:upordown,  keyword:current_keyword, error: null});
       console.log('Long Mongo Request took:', new Date() - start, 'ms');

       return(res);

                }
            })
        }})});


app.post('/', function(req, res) {
  var start = new Date();

  if (req.body == '') res.status(400).send("Please send a proper keyword");
  MongoClient.connect(mongoUrl, function (err, db) {
      if (err) throw 'Error connecting to database - ' + err;
      else{
	       current_keyword = req.body.keyword;
	          access.findKeywordWithCache(db, redis, req.body.keyword, function(keywords, cache_or_redis){
              if (!keywords) res.status(500).send('Could not find in Database')
              else{
	        var ordered = Object.keys(keywords)
	        var newordered = ordered.sort(function compare(a,b){
			    var datea = new Date(a)
			    var dateb = new Date(b)
			    return datea - dateb;
		      });

		var xval = "";
		var yval = [];

		newordered.forEach(function(key) {
		  xval = xval.concat(",\'", key, "\'");
		  yval.push(keywords[key]);
		});
	     xval = xval.substring(1)
       if (cache_or_redis == "redis"){
           // this is just the ratio
           var y_first = yval[1];
           var y_last = yval.slice(-1)[0];
           var ratio = y_last / y_first
           var upordown = "not changed"
           if  (ratio >1){
             upordown = "increased"
           }else{
             upordown = "decreased"
           }
           // formatting
           ratio = (ratio-1) * 100;
           ratio = ratio.toFixed(1)
           var howlong =  new Date() - start;

           res.render('result', {xval: xval, yval: yval, howlong: howlong, ratio:ratio, upordown:upordown,  keyword:current_keyword, error: null});
         }}
       })
   }});
        });


app.listen(3000, function () {
  console.log('OldNews app listening on port 3000!')
});
