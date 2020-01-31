var express = require('express');
var router = express.Router();
var mongodb = require('mongodb');

var access = require('../access.js');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: '#OldNews' });
});

router.get("/thelist", function(req, res){
  var MongoClient = mongodb.MongoClient;
  var url = 'mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com:27017/pymongo_test';

  var redisClient = require('redis').createClient;
  var redis = redisClient(6379, 'redis-clust.wdeoii.ng.0001.usw2.cache.amazonaws.com');


  MongoClient.connect(url, function(err, db){
  if(err){
    console.log("Unable to connect to database. Sorry...", err);
  }else{
    console.log("Connection established");
    var mydb = db.db("tweet_db");
    //var mydb = db("pymongo_test");
    var mycollection = mydb.collection("Keywords");
    mycollection.find({}).toArray(function(err, result){
       if (err){
         res.send(err);
       } else if (result.length){
	 res.render('thelist', {
	  "thelist" : result });
	} else{
	  res.send('No tweets found');
	}
	db.close();
      });
   }
  });
});
module.exports = router;
