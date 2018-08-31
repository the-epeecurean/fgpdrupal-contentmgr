const Client = require('node-rest-client').Client;
const chalk = require('chalk');
const mysql = require('mysql');
const fs = require('fs');
const forEP = require('foreach-promise');
const connxnFile = './awsconnect.json';
let database;

//Declare a new Client object for handling REST requests through Node
var client = new Client();

//Array for all Topic Categories that will be requested from the GeoNetwork REST
var topicCats = [
	'biota',
	'boundaries',
	'climatologyMeteorologyAtmosphere',
	'economy',
	'elevation',
	'environment',
	'farming',
	'geoscientificInformation',
	'health',
	'imageryBaseMapsEarthCover',
	'inland waters',
	'intelligence',
	'location',
	'oceans',
	'planningCadastre',
	'society',
	'structure',
	'transportation',
	'utilitiesCommunication'	
];
let nbrCats = topicCats.length;

//Set up an object for holding all GeoNetwork records for the current topic that have an Application
var topicDist = [];
var topicIdent = [];
var args;
var thisRecord;
var topicsProcessed = 0;
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

function init() {
	return new Promise(function(resolve, reject) {
		fs.readFile(connxnFile, (err, params) => {
			if (err) {
				console.log(chalk.bgRed.white("Error reading credentials file; cannot connect to db."));
				return reject(err);
			} else {
				resolve(JSON.parse(params));
			}
		})
	})
};

class Database {
	constructor(config) {
		this.connection = mysql.createConnection(config);
		console.log(chalk.bgGreen.white("Connected to database!"));
	}
	query(sql, args) {
		return new Promise((resolve, reject) => {
			this.connection.query(sql, args, (err, rows) => {
				if(err)
					return reject(err);
				resolve(rows);
			});
		});
	}
	close() {
		return new Promise((resolve, reject) => {
			this.connection.end(err => {
				if (err)
					return reject(err);
				resolve();
			});
		});
	}
}

function restCall(topicCats, result) {
	const argsList = [];
	
	return new Promise( (resolve, reject) => {
		forEP(topicCats, (n, index, array)=> {
			//Connect using mySQL uswing connection arguments
			database = new Database(result);
				
			const args = {
				parameters: {
					topicCat: n,
					fast: "false"
				},
				headers: { "Content-Type": "application/json" }
			};

			gnQuery(args, n);
		})
		.then( ()=> resolve())
		.catch(function(err) {
			console.log(chalk.bgBlue.white("Error during for-each-promise loop:  "+err));
		});

	});
};

//SQL SELECT query to check AWS db for entry with same UUID and titleEN;
//Prevent duplicates from being created in future runs of the script. Will check all GeoNetwork records, but only add new additions to the content database.
async function awsDBcheck(uuid, titleEN, topicCat, topicDist, topicIdent, rec, last, callback) {
	
	return new Promise( (resolve, reject) => {
		//console.log(last);
		database.query("SELECT * FROM fgpwp_content WHERE uuid='"+uuid+"' AND titleEN='"+titleEN+"' AND topic='"+topicCat+"'")
			.then(rows => {
				if (rows != '') {
					thisRecord = "exists";
				} else {
					thisRecord = "new";
				}
				return callback(uuid, topicDist, topicIdent, rec, thisRecord);
			})
			.then( () => {
				resolve(callback(uuid, topicDist, topicIdent, rec, thisRecord));
			})
			.then( () => {
				if (last == true) {
					database.close();
				}
			})
			.catch(function(err) {
				console.log(chalk.bgRed.blue(uuid+"   "+topicCat+"Error in checking DB record call:  "+err));
			});
	});
}

//SQL INSERT query; adds application record to AWS MySQL database.
async function awsDBwrite(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat) {
	return new Promise( (resolve, reject) => {
		//Insert a new content table row for the FGP entry
		database.query('INSERT INTO fgpwp_content (uuid, thumbnailURL, titleEN, bodyEN, keywordsEN, titleFR, bodyFR, keywordsFR, topic) VALUES ("'+uuid+'", "'+thumb+'", "'+titleEN+'", "'+bodyEN+'", "'+keysEN+'", "'+titleFR+'", "'+bodyFR+'", "'+keysFR+'", "'+topicCat+'")')
			.then(rows => {
				return "Record added";
				//return callback(uuid, titleEN, topicCat);
			})
			.then( () => {
				console.log(chalk.bgGreen.yellow("Recorded added for: "+uuid+" - "+titleEN));
				resolve();
				//resolve(callback(uuid, titleEN, topicCat));
			})
			.catch(function(err) {
				console.log(chalk.bgRed.white("Error in inserting DB record call:  "+err));
			});
	});		
}

//Construct REST call, and write to AWS content db based on checks to database to ensure content isn't duplicated.
async function gnQuery(args, topicCat) {
		
		var loggedApps = [];
		var filteredRecord;
		
		client.post("https://maps-dev.canada.ca/geonetwork/srv/csw/q", args, async function (data, response) {
			topicsProcessed++;
			topicDist = [];
			topicIdent = [];
			//Select from response all metadata record objects and determine how many records exist in this topic category
			var gnRecords = data.response['gmd:MD_Metadata'];
			if(gnRecords) {
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
										}
									}
								} else {
									if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
										topicDist.push(gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine']);
										topicIdent.push(gnRecords[i]);
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
				var last = false;
				//Loop through entries with Applications and extract fields needed to generate content on Drupal end
				for (var j = 0; j < topicDist.length; j++) {
					uuid = topicIdent[j]['gmd:fileIdentifier']['gco:CharacterString'];
					titleEN = topicDist[j]['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'];
					var shortTitle = titleEN.substr(0, titleEN.indexOf('('));
					
					if ((j+1) == topicDist.length && topicsProcessed == nbrCats) {
						last = true;
					}
					if (!loggedApps.includes(shortTitle) || last == true) {
						loggedApps.push(shortTitle);					
						awsDBcheck(uuid, titleEN, topicCat, topicDist, topicIdent, j, last, function(uuid, topicDist, topicIdent, rec, result) {
							if (result == "exists") {
								//Skip this record if result returns exists after checking AWS database.
								console.log(chalk.bgRed.white("Record "+uuid+" already exists in AWS DB."));
							} else if (result == "new") {
								console.log(chalk.bgGreen.white("Record "+uuid+" needs to be created in AWS DB."));
								//Gather fields needed for Drupal content entry
								//NEED: Unique ID, English Title, French Title, Thumbnail, English App URL, French App URL, English Keywords, French Keywords
								titleEN = topicDist[rec]['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'];
								bodyEN = bodyEN = "<iframe src='"+topicDist[rec]['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%'></iframe>";
								if (topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:graphicOverview'] && topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:graphicOverview']['gmd:MD_BrowseGraphic']) {
									thumb = topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:graphicOverview']['gmd:MD_BrowseGraphic']['gmd:fileName']['gco:CharacterString'];
								} else {
									thumb = '';
								}
								//Find a record with the same short title as the titleEN and a description that includes "fra" and add that as the titleFR and bodyFR
								for (var fr = 0; fr < topicDist.length; fr++) {
									var engSubtitle = titleEN.substr(0, titleEN.indexOf('('));
									if (topicDist[fr]['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'].includes(engSubtitle) && topicDist[fr]['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("fra")) {
										titleFR = topicDist[fr]['gmd:CI_OnlineResource']['gmd:name']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
										bodyFR = "<iframe src='"+topicDist[fr]['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%'></iframe>";
									}
								}
								
								//Check if the metadata record has descriptive keywords entered
								if(topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']) {
									//Handling variations in structure of child tags under ['gmd:descriptiveKeywords']
									//Differences in number of MD_Keywords, whether MD_Keywords tag is parent to more than one keyword, or if only one keyword is used.
									if (topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'].length) {
										nbrDescKeywords = topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'].length;
										//Multiple ['gmd:descriptiveKeywords'] with different structures
										for (var descKeys = 0; descKeys < nbrDescKeywords; descKeys++) {
											if (topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'].length) {
												nbrKeywords = topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'].length;
												//Loop through descriptiveKeywords to create keywords text string
												for (var keys = 0; keys < nbrKeywords; keys++) {
													if (topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gco:CharacterString']) {
														keysEN += topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gco:CharacterString']+", ";
													}
													if (topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_']) {
														keysFR += topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_']+", ";
													}
												}
											} else {
												keysEN = topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword']['gco:CharacterString'];
												keysFR = topicIdent[rec]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
											}
										}
									}
								}

								//Write new record to database
								//console.log(chalk.bgBlue.yellow(uuid+"   "+thumb+"   "+titleEN+"   "+titleFR+"   "+bodyEN+"   "+bodyFR+"   "+keysEN+"   "+keysFR+"   "+topicCat)); //For debugging; can remove
								//Need to make sure this uuid, titleEN pair have not already been written during this runtime.
								var valCheck = [uuid, titleEN];
								var v = JSON.stringify(valCheck);
								var z = JSON.stringify(dbCheck);
								var vzCheck = z.indexOf(v);

								if(vzCheck != -1) {
									console.log("Already processed "+uuid+", "+titleEN+"!");
								} else {
									try {
										awsDBwrite(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat);
										dbCheck.push([uuid, titleEN]);
									}
									catch(err) {
										console.log(chalk.bgRed.yellow("Error in trying to insert record to AWS database."));
									}
								}
							}
							
						});
						
						
					} else {
						continue;
					}
				}
			} else {
				
			}
		});
}

function awsDBconnect() {
	var config = init();
	config.then(async function(result) {
		
		restCall(topicCats, result)
		
	}, function(err) {
		console.log(chalk.bgRed.white("Error connecting to DB  "+err));
	})
	.catch(function(err) {
		console.log(chalk.bgBlue.white("Error2 communicating with database:  "+err));
	});
};

awsDBconnect();