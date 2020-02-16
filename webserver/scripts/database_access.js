

module.exports.findKeywordWithCache = function (db, redis, keyword, callback) {
    redis.get(keyword, function (err, reply) {
        if (reply == undefined){
            callback(null, "redis");
        } else {
            callback(JSON.parse(reply), "redis");

        }
    });
};



  module.exports.findKeywordWithMongoOnly_longterm = function (db, keyword, callback) {
      var mydb = db.db("all_tweets");
      console.log("from full database")

      var mycollection = mydb.collection("tweets_zlib");
      mycollection.findOne({
        "Keyword": keyword
      }, function(err, doc){
        if (err || !doc) callback(null);
        else{
          callback(doc["Time"]);
        }})
    };
