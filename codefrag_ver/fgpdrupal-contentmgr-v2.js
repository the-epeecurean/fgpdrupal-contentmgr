const Client = require('node-rest-client').Client;
const chalk = require('chalk');
const mysql = require('mysql');
const fs = require('fs');
const async = require('async');
const forEP = require('foreach-promise');
const connxnFile = './awsconnect.json';

//Declare a new Client object for handling REST requests through Node
var client = new Client();

//Array for all Topic Categories that will be requested from the GeoNetwork REST
var topicCats = [
	//'biota',
	'boundaries',
	//'climatology, meteorology, atmosphere',
	//'economy',
	//'elevation',
	//'environment',
	'farming'
	//'geoscientificInformation',
	//'health',
	//'imageryBaseMapsEarthCover',
	//'inland waters',
	//'intelligence',
	//'location',
	//'oceans',
	//'planning cadastre',
	//'society',
	//'structure',
	//'transportation',
	//'utilities communication'	
];

//Set up an object for holding all GeoNetwork records for the current topic that have an Application
var topicDist = [];
var topicIdent = [];
var args;
var thisRecord;
var contentProcessed = 0;
//Variables to hold entry fields to output to Drupal content
var uuid;
var thumb;
var titleEN;
var bodyEN;
var keysEN;
var titleFR;
var bodyFR;
var keysFR;
var topicCat;
let con;

function init() {
	return new Promise(function(resolve, reject) {
		fs.readFile(connxnFile, (err, params) => {
			if (err) {
				console.log(chalk.bgRed.white("Error reading credentials file; cannot connect to db."));
			} else {
				resolve(JSON.parse(params));
			}
		})
	})
	
	
};

function restCall(topicCats) {
	const argsList = [];
	
	return new Promise( (resolve, reject) => {
		forEP(topicCats, (n, index, array)=> {
			const args = {
				parameters: {
					topicCat: n,
					fast: "false"
				},
				headers: { "Content-Type": "application/json" }
			};

			var apps = gnQuery(args, n);
			resolve(apps);
		});
	});
};

function awsDBconnect() {
	var initializeConn = init();
	initializeConn.then(async function(result) {
		//Connect using mySQL uswing connection arguments
		con = mysql.createConnection(result);

		await con.connect(function(err) {
			if (err) return console.log(chalk.bgRed.white("Error connecting to DB  "+err));
			console.log(chalk.bgGreen.white("Connected to database!"));
		});
		
		//resolve();
	}, function(err) {
		console.log(chalk.bgRed.white("Error connecting to DB  "+err));
	})
	.then(async function() {
		await restCall(topicCats)
	})
	//.then(async function() {
	//		//appCheck(result);
	//	await awsDBdisconnect();
	//})
	.catch(function(err) {
		console.log(chalk.bgBlue.white("Error communicating with database:  "+err));
	});
};

//SQL SELECT query to check AWS db for entry with same UUID and titleEN;
//Prevent duplicates from being created in future runs of the script. Will check all GeoNetwork records, but only add new additions to the content database.
async function awsDBcheck(uuid, titleEN,topicCat, callback) {
	await con.query(
		"SELECT * FROM fgpwp_content WHERE uuid='"+uuid+"' AND titleEN='"+titleEN+"' AND topic='"+topicCat+"'",
		function (error, results, fields) {
			if (error) {
				console.log(chalk.bgRed.white("Error2:" ) );
				console.log(error);
				return;
			}
			//console.log(chalk.bgGreen.white("Count:", results.length));
			if (results != '') {
				thisRecord = "exists";
			} else {
				thisRecord = "new";
			}
			return callback(uuid, thisRecord);
		}
	);
}

//Function to disconnect from AWS database using NodeJS mysql
async function awsDBdisconnect() {
	//Disconnect from the AWS database
	con.end((err) => {
		if(err) {
			console.log(chalk.bgRed.white("error disconnecting."));
		} else {
			//Connection should terminate gracefully
			//Should ensure all enqueued queries complete
			//before sending a COM_QUIT packet to the MySQL server
			console.log(chalk.bgGreen.white("Database connection dropped!"));
		}

	});
	
}

//Construct REST call, and write to AWS content db based on checks to database to ensure content isn't duplicated.
async function gnQuery(args, topicCat) {
		topicDist = [];
		topicIdent = [];
		var loggedApps = [];
		var filteredRecord;
		
		client.post("https://maps-dev.canada.ca/geonetwork/srv/csw/q", args, function (data, response) {
			//Select from response all metadata record objects and determine how many records exist in this topic category
			var gnRecords = data.response['gmd:MD_Metadata'];
			var nbrGNRecords = gnRecords.length;
			console.log(chalk.bgGreen.white("topicCat is currently "+topicCat+". There are "+nbrGNRecords+" records returned."));
			
			//Loop through records returned for this category and filter to only "Applications".
			for ( var i = 0; i < nbrGNRecords; i++) {
				//Handle variance in ['gmd:transferOptions'] tags
				//If the current metadata record has a ['gmd:transferOptions'], delve further. If not, we will skip this record.
				if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions']) {
					recDigitalOptions = gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'].length;
					
					//Loop through each transferOptions type entered for this metadata record.
					for (var r = 0; r < recDigitalOptions; r++) {
						try {
							if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length) {
								transferOptions = gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length;
								for (var dto = 0; dto < transferOptions; dto++) {
									//If this transferOption has "Application" listed, add it to the subset of records we will append to the Drupal content database.
									if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][dto]['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
										topicDist.push(gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][dto]);
										topicIdent.push(gnRecords[i]);
										//Stop looping through this record's transferOptions if it is already flagged as having an Application.
										//Proceed to the next metadata record.
										//break LoopRec;
										//}
									}
								}
							} else {
								if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
									topicDist.push(gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine']);
									topicIdent.push(gnRecords[i]);
									//Stop looping through this record's transferOptions if it is already flagged as having an Application.
									//Proceed to the next metadata record.
									//break LoopRec;
								}
							}
						} catch (err) {
							console.log(chalk.bgRed.white("Metadata tag parsing error occurring on uuid "+gnRecords[i]['gmd:fileIdentifier']['gco:CharacterString']));
							console.log(chalk.bgRed.white(err));
						}
					}
				}
			}
			
			//State in console how many of processed entries for this topicCat have identified an Application among their resources.
			console.log(chalk.bgGreen.white(topicDist.length+' '+topicCat+' entries have Applications listed.'));
			
			var loggedApps = [];
			var dbCheck = [];
			//Loop through entries with Applications and extract fields needed to generate content on Drupal end
			for (var j = 0; j < topicDist.length; j++) {
				uuid = topicIdent[j]['gmd:fileIdentifier']['gco:CharacterString'];
				titleEN = topicDist[j]['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'];
				thumb = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:graphicOverview']['gmd:MD_BrowseGraphic']['gmd:fileName']['gco:CharacterString'];
				var shortTitle = titleEN.substr(0, titleEN.indexOf('('));
				if (!loggedApps.includes(shortTitle)) {
					loggedApps.push(shortTitle);
					console.log(uuid);
					awsDBcheck(uuid, titleEN, topicCat, function(uuid, result) {
						if (result == "exists") {
							console.log(chalk.bgRed.white("Record "+uuid+" already exists in AWS DB."));
							//Skip this record if result returns exists after checking AWS database.
						} else if (result == "new") {
							console.log(chalk.bgGreen.white("Record "+uuid+" needs to be created in AWS DB."));
							//Write new record to database
						}
						
					});
					
					dbCheck.push({"uid": uuid, "titleEN": titleEN});
				} else {
					continue;
				}
			}

		});
}

awsDBconnect();