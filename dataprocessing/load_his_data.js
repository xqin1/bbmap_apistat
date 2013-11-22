var MongoClient = require('mongodb').MongoClient,
	Server = require('mongodb').Server,
	ObjectID = require('mongodb').ObjectID
	moment = require('moment')
	async = require('async'),
	fs = require('fs');
var apiStatSummaryDoc = {
	"_id" : "", //2011091100
	"count":0,
	"apiCountByLocation":{
			 "CAI-findClosestByCoordinates" : [], //array length = hex number, 2835
 			"Broadband-findWirelineServiceByCoordinates":[], //array length = hex number, 2835
 			"Demographic-findByCordinates":[], //array length = hex number, 2835
 			"Broadband-findWirelessServiceByCoordinates":[],//array length = hex number, 2835
 			"Census-findByCoordinates":[] //array length = hex number, 2835
	},
	"apiCountByName":{
			"CAI-findClosestByCoordinates" : {"count":0,"responseTime":0},
 			"Broadband-findWirelineServiceByCoordinates": {"count":0,"responseTime":0},
 			"Demographic-findByCordinates":{"count":0,"responseTime":0},
 			"Broadband-findWirelessServiceByCoordinates": {"count":0,"responseTime":0},
 			"Census-findByCoordinates": {"count":0,"responseTime":0},
			 "ProviderFeedback-findProviderFeedbackByBlockId" : {"count":0,"responseTime":0},
 			 "Provider-getAllProviders": {"count":0,"responseTime":0},
 			 "Census-findGeographyByFips" : {"count":0,"responseTime":0},
 			"Almanac-findAllRankingsByGeographyIdWithinState": {"count":0,"responseTime":0},
 			"Almanac-findRankingsByGeographyIdWithinNation": {"count":0,"responseTime":0},
 			"BIP-findByNation": {"count":0,"responseTime":0},
 			"Almanac-getAlmanacParameters": {"count":0,"responseTime":0},
 			"SpeedTest-findForNation": {"count":0,"responseTime":0},
 			"SpeedTest-findMinAndMaxQuartileSpeedsByGeographyType": {"count":0,"responseTime":0},
 			"BIP-findByStateIds": {"count":0,"responseTime":0},
 			"GEOGRAPHY-findGeographiesByStateId": {"count":0,"responseTime":0},
 			"Almanac-findAllRankingsWithinState": {"count":0,"responseTime":0},
 			"Provider-findByProviderWithinNation": {"count":0,"responseTime":0},
 			"Provider-findSimilarProvidersWithinNation": {"count":0,"responseTime":0},
 			"BroadbandSummary-findBroadbandSummaryByNation": {"count":0,"responseTime":0},
 			"Demographic-findForNation": {"count":0,"responseTime":0},
 			"BroadbandSummary-findBroadbandSummaryByGeographyId": {"count":0,"responseTime":0},
 			"CAI-findByGeographyTypeAndId": {"count":0,"responseTime":0},
 			"Demographic-findByGeographyTypeAndId": {"count":0,"responseTime":0},
 			"GEOGRAPHY-findGeographiesByType": {"count":0,"responseTime":0},
 			"Provider-findProvidersByNameWithinGeography": {"count":0,"responseTime":0},
 			"Provider-findByProviderWithinGeography": {"count":0,"responseTime":0},
 			"GEOGRAPHY-findGeographyByStateAndName": {"count":0,"responseTime":0},
 			"Provider-findProvidersWithinGeography": {"count":0,"responseTime":0},
 			"Provider-findSimilarProvidersWithinGeography": {"count":0,"responseTime":0},
			 "Almanac-findAllRankingsWithinNation": {"count":0,"responseTime":0},
			 "CAI-findCAIForNation": {"count":0,"responseTime":0},
 			"Provider-findByProviderWithinState": {"count":0,"responseTime":0},
 			"Provider-findProvidersWithinState": {"count":0,"responseTime":0},
 			"Provider-findSimilarProvidersWithinState": {"count":0,"responseTime":0},
 			"BIP-findByStateNames": {"count":0,"responseTime":0},
 			"Provider-findProvidersByName": {"count":0,"responseTime":0},
 			"BTOP-findByStateIds": {"count":0,"responseTime":0},
 			"BTOP-findByStateNames": {"count":0,"responseTime":0},
 			"Census-findGeographyByName": {"count":0,"responseTime":0},
 			"CAI-findByGeographyTypeAndName": {"count":0,"responseTime":0},
 			"GEOGRAPHY-findGeographyById": {"count":0,"responseTime":0},
 			"SpeedTest-findByGeographyTypeAndId": {"count":0,"responseTime":0},
 			"SpeedTest-findByGeographyTypeAndName": {"count":0,"responseTime":0},
 			"Provider-getProviderParameters": {"count":0,"responseTime":0},
 			"BTOP-findByNation": {"count":0,"responseTime":0},
 			"Demographic-findByGeographyTypeAndName": {"count":0,"responseTime":0},
 			"GEOGRAPHY-findGeographyByName": {"count":0,"responseTime":0},
 			"Provider-findProvidersWithinNation": {"count":0,"responseTime":0},
			 "Provider-findProvidersByNameWithinNation": {"count":0,"responseTime":0},
 			"AVAILABILITY-findByStateId": {"count":0,"responseTime":0},
 			"AVAILABILITY-findByCountyName": {"count":0,"responseTime":0},
 			"AVAILABILITY-findByCountyIds": {"count":0,"responseTime":0},
 			"AVAILABILITY-findByStateName": {"count":0,"responseTime":0},
 			"AVAILABILITY-findByNation":{"count":0,"responseTime":0}
		}
}

var numCount = 0;
var processStart = moment();
var mongoClient = new MongoClient(new Server('localhost', 27017,
										{'native_parser' : true}));
var db = mongoClient.db('gisdb');
var apiStat = db.collection("apistatsmessagedocs");
var apiStatSummary = db.collection("apistatssummary");
var hex75k = db.collection("hex_75k");


var stopTime = moment("2013-09-12 16:00:00").unix();//.seconds();
//var stopTime = moment("2011-09-28 12:00:00").unix();//.seconds();
var stopID =new ObjectID.createFromTime(stopTime);
var projection = {"apiName":1, "latlng":1, "date":1, "isGeospatialAPI":1,"responseTime":1};
var spatialProjection = {"_id":0,"properties.gid":1};

var rs = fs.createWriteStream('../log/dataprocessing.log',{flags:'a'});


mongoClient.open(function(err,mongoClient){
	if (err) throw err;
	console.log("mongo client connected");

	var startTime = moment("2011-09-27 11:00:00");
	//at beginning of the hour, create the new document, delte if existes
	var q = {"_id": {"$gte": startTime.unix(), "$lte": stopTime}};
	apiStatSummary.remove(q,function(err,result){
		if (err) throw err;
		processHourlyData();
	})


	function processHourlyData(){
		
		if (startTime.unix() > stopTime){
			var processEnd = moment();
			console.log("client closed. Process " + numCount + " records takes " + processEnd.diff(processStart, 'minutes') + " minutes");
			mongoClient.close();
			rs.end();
		} else{
			

			var msg = "processing data from " + startTime.toString() + " - ";
			//create new document
			var newSummaryDoc = {"_id": startTime.unix(),"count":0, "apiCountByLocation":{}, "apiCountByName":{}};

			var startID =new ObjectID.createFromTime(startTime.unix());
			//start time now increase by 1 hour
			var endID =  new ObjectID.createFromTime(startTime.add('h',1).unix());
			var query = {"_id": {"$gte": new ObjectID(startID.toHexString()), "$lt": new ObjectID(endID.toHexString())}};
			
			rs.write(JSON.stringify(query) + "\n");
			msg += startTime.toString();
			console.log(msg);
			rs.write(msg + "\n");

			var cursor = apiStat.find(query,projection);
			cursor.toArray(function(err,results){
				if (err) {console.log("error");congoClient.close();throw err};

				numCount += results.length;

				console.log("number of records: " + results.length + ", total recoreds:" + numCount);
				rs.write("number of records: " + results.length + ", total recoreds:" + numCount + "\n");

				function iterator(r,callback){
					newSummaryDoc.count++;
					if (typeof newSummaryDoc.apiCountByName[r.apiName] == "undefined"){
						newSummaryDoc.apiCountByName[r.apiName] = {};
						newSummaryDoc.apiCountByName[r.apiName].count = 1;
						newSummaryDoc.apiCountByName[r.apiName].responseTime = r.responseTime;
					}
					else{
						newSummaryDoc.apiCountByName[r.apiName].count++;
						newSummaryDoc.apiCountByName[r.apiName].responseTime = (newSummaryDoc.apiCountByName[r.apiName].responseTime * (newSummaryDoc.apiCountByName[r.apiName].count-1) 
																								+ r.responseTime)/newSummaryDoc.apiCountByName[r.apiName].count;
					}
					if (r.isGeospatialAPI && !isNaN(r.latlng.long) && !isNaN(r.latlng.lat) && r.latlng.lat < 90 && r.latlng.lat > -90 && r.latlng.long>-180 && r.latlng.long<180){
						var spatialQuery = {geometry:{$geoIntersects:{$geometry:{type:"Point",coordinates:[r.latlng.long,r.latlng.lat]}}}};
						hex75k.findOne(spatialQuery,spatialProjection,function(err, hex){
							if (err) {throw err};
							if (hex != null){
								if (typeof newSummaryDoc.apiCountByLocation[r.apiName] == "undefined"){
									newSummaryDoc.apiCountByLocation[r.apiName] = {};
								}

								if (typeof newSummaryDoc.apiCountByLocation[r.apiName][hex.properties.gid] == "undefined"){
									newSummaryDoc.apiCountByLocation[r.apiName][hex.properties.gid] = 1;
								}else{
									newSummaryDoc.apiCountByLocation[r.apiName][hex.properties.gid]++;
								}
								//console.log(hex)
							}
							callback();
						});
					}else{
						callback();
					}
				}

				async.forEach(results,iterator,done);



				// results.forEach(function(r){
				// 	newSummaryDoc.count++;
				// 	if (typeof newSummaryDoc.apiCountByName[r.apiName] == "undefined"){
				// 		newSummaryDoc.apiCountByName[r.apiName] = {};
				// 		newSummaryDoc.apiCountByName[r.apiName].count = 1;
				// 		newSummaryDoc.apiCountByName[r.apiName].responseTime = r.responseTime;
				// 	}
				// 	else{
				// 		newSummaryDoc.apiCountByName[r.apiName].count++;
				// 		newSummaryDoc.apiCountByName[r.apiName].responseTime = (newSummaryDoc.apiCountByName[r.apiName].responseTime * (newSummaryDoc.apiCountByName[r.apiName].count-1) 
				// 																				+ r.responseTime)/newSummaryDoc.apiCountByName[r.apiName].count;
				// 	}

				// 	//location query
				// 	if (r.isGeospatialAPI){
				// 		var spatialQuery = {geometry:{$geoIntersects:{$geometry:{type:"Point",coordinates:[r.latlng.long,r.latlng.lat]}}}};
				// 		hex75k.findOne(spatialQuery,spatialProjection,function(err, hex){
				// 			if (err) throw err;
				// 			if (hex != null){
				// 				if (typeof newSummaryDoc.apiCountByLocation[r.apiName] == "undefined"){
				// 					newSummaryDoc.apiCountByLocation[r.apiName] = {};
				// 				}

				// 				// if (typeof newSummaryDoc.apiCountByLocation[r.apiName][hex.properties.gid] == "undefined"){
				// 				// 	newSummaryDoc.apiCountByLocation[r.apiName][hex.properties.gid] = 1;
				// 				// }else{
				// 				// 	newSummaryDoc.apiCountByLocation[r.apiName][hex.properties.gid]++;
				// 				// }
				// 				console.log(hex)
				// 			}
				// 		})
				// 	}
					
				// })
				//insert new doc
				function done(err){
					if (err) throw err;

					apiStatSummary.insert(newSummaryDoc, function(err, result){
						if (err) throw err;
						processHourlyData();
					})
				}
			//processHourlyData();

			})
		}
	}
});