

  module.exports.findKeywordWithCache = function (db, redis, keyword, callback) {
      redis.get(keyword, function (err, reply) {
          if (err) callback(null);
          else if (reply){
              console.log("from cache")
              callback(JSON.parse(reply), "redis");
          } else {
              //Book doesn't exist in cache - we need to query the main database
              var mydb = db.db("all_tweets");
              mydb.collection('tweets_zlib').findOne({
                  "Keyword": keyword
              }, function (err, doc) {
                  if (err || !doc) callback(null);
                  else {
                      redis.set(keyword, JSON.stringify(doc), function () {
                          callback(doc, "mongo");
                      });
                  }
              });
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
