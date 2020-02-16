// ensures all timestamps are
module.exports = {

   ordertimestamps: function (keywords){
    var ordered = Object.keys(keywords)
    var toreturn = ordered.sort(function compare(a,b){
      var datea = new Date(a)
      var dateb = new Date(b)
      return datea - dateb;
    });
    return(toreturn)
  },

  calculate_x_y_values: function (orderedkeywords, keywords){

    	   var xval = "";
    		 var yval = [];

    		orderedkeywords.forEach(function(key) {
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

          return([String(xval), String(yval), ratio, upordown])
  }
};
