module.exports.findKeywordByCached = function (db, redis, keyword, callback) {
    redis.get(keyword, function (err, reply) {
        if (err) callback(null);
        else if (reply) //Book exists in cache
        callback(JSON.parse(reply));
        else {
            //Book doesn't exist in cache - we need to query the main database
            db.collection('text').findOne({
                keyword: keyword
            }, function (err, doc) {
                if (err || !doc) callback(null);
                else {//Keyword found in database, save to cache and return to client
                    redis.set(title, JSON.stringify(doc), function () {
                        callback(doc);
                    });
                }
            });
        }
    });
};
