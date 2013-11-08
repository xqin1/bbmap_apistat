var MongoClient = require('mongodb').MongoClient,
	Server = require('mongodb').Server,
	ObjectID = require('mongodb').ObjectID;


var mongoClient = new MongoClient(new Server('localhost', 27017,
										{'native_parser' : true}));
var db = mongoClient.db('gisdb');
var apiStat = db.collection("apistatsmessagedocs");

var startTime = new Date("09/21/2012 00:00:00").getTime()/1000;
var endTime = new Date("09/21/2012 16:00:00").getTime()/1000;

var startID =new ObjectID.createFromTime(startTime);
var endID =  new ObjectID.createFromTime(endTime);
console.log(startID.toHexString() + " " + endID);

//var query = {"_id": {"$gt": new ObjectID("4e79fda4091461c8453d1e63")}};
var query = {"_id": {"$gt": new ObjectID(startID.toHexString()), "$lt": new ObjectID(endID.toHexString())}};
var projection = {"apiName":1, "latlng":1, "date":1, "isGeospatialAPI":1};
var options = {"limit" : 10};

mongoClient.open(function(err,mongoClient){
	if (err) throw err;
	console.log("mongo client connected");

	apiStat.find(query,projection,options).toArray(function(err,results){
		if (err) {console.log("error");congoClient.close();throw err};

		results.forEach(function(r){
			console.log(r.date);
		})
		mongoClient.close();
	})
});