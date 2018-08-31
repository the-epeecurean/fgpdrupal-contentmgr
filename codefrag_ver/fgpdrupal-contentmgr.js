// Declare a new Client object, requiring the node-rest-client NodeJS package
var Client = require('node-rest-client').Client;
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
var topicApps = [];
var args;
var thisRecord;
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
var contentProcessed = 0;

//Use mysql package and connect to AWS DB
var chalk = require("chalk");
var mysql = require('mysql');

function awsDBconnect() {//var awsDBconnect = new Promise(function(resolve, reject) {
	con = mysql.createConnection({
		host: "fgp-drupal-02.cc0hbqeyyxxw.ca-central-1.rds.amazonaws.com",
		ssl: "Amazon RDS",
		user: "admin",
		password: "password",
		database: "gnFGPentries"
	});
	
	con.connect(function(err) {
		if (err) throw err;
		console.log("Connected to database!");
	});
	
};//);

//SQL INSERT query; add application record to AWS MySQL database.
function awsDBwrite(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat) {
	//Insert a new content table row for the FGP entry
	awsDBconnect();
	//console.log(chalk.bgRed.white("Creating DB entry for ", titleEN, " in category ", topicCat));
	con.query(
		'INSERT INTO fgpwp_content (uuid, thumbnailURL, titleEN, bodyEN, keywordsEN, titleFR, bodyFR, keywordsFR, topic) VALUES ("'+uuid+'", "'+thumb+'", "'+titleEN+'", "'+bodyEN+'", "'+keysEN+'", "'+titleFR+'", "'+bodyFR+'", "'+keysFR+'", "'+topicCat+'")',
		function(error, records, fields) {
			if(error) {
				console.log(chalk.bgRed.white("Error1:" ) );
				console.log(error);
				return;
			}
			//console.log(chalk.bgGreen.white("Count:", records.length," and record for ", titleEN," in ", topicCat," created"));
		}
	);
	
	awsDBdisconnect();

}

//SQL SELECT query to check AWS db for entry with same UUID and titleEN;
//Prevent duplicates from being created in future runs of the script. Will check all GeoNetwork records, but only add new additions to the content database.
function awsDBcheck(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat, callback) {
	//Check the Drupal content table to make sure the current uuid/application-name pair don't already exist as content
	awsDBconnect();
	con.query(
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
			return callback(thisRecord, uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat);
		}
	);
	
	awsDBdisconnect();
}

//Function to disconnect from AWS database using NodeJS mysql
function awsDBdisconnect() {
	//Disconnect from the AWS database
	con.end((err) => {
		if(err) {
			console.log("error disconnecting.");
		}
		//Connection should terminate gracefully
		//Should ensure all enqueued queries complete
		//before sending a COM_QUIT packet to the MySQL server
	});
	console.log("Database connection dropped!");
	
}	

//Construct REST call, and write to AWS content db based on checks to database to ensure content isn't duplicated.
function gnQuery(args, topicCat) {

	topicApps = [];
	client.post("https://maps-dev.canada.ca/geonetwork/srv/csw/q", args, function (data, response) {
		//Select from response all metadata record objects and determine how many records exist in this topic category
		var gnRecords = data.response['gmd:MD_Metadata'];
		var nbrGNRecords = gnRecords.length;

		//Loop through records returned for this category and filter to only "Applications".
		for ( var i = 0; i < nbrGNRecords; i++) {
			//Can delete later - for code debugging purposes - lists uuids of entries in this topicCat.//
			//console.log(i+" - "+gnRecords[i]['gmd:fileIdentifier']['gco:CharacterString']);
			
			//If the current metadata record has a ['gmd:transferOptions'], delve further. If not, we will skip this record.
			if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions']) {
				recDigitalOptions = gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'].length;
				
				//Loop through each transferOptions type entered for this metadata record.
				LoopRec:
				for (var r = 0; r < recDigitalOptions; r++) {
					
					//Check whether there is an array of ['gmd:onLine'] objects, or whether there is just one ['gmd:onLine'] entry.
					if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length) {
						transferOptions = gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length;
						for (var dto = 0; dto < transferOptions; dto++) {
							//If this transferOption has "Application" listed, add it to the subset of records we will append to the Drupal content database.
							if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][dto]['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
								topicApps.push(gnRecords[i]);
								//Stop looping through this record's transferOptions if it is already flagged as having an Application.
								//Proceed to the next metadata record.
								break LoopRec;
							}
						}
					} else {
						if (gnRecords[i]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][r]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
							topicApps.push(gnRecords[i]);
							//Stop looping through this record's transferOptions if it is already flagged as having an Application.
							//Proceed to the next metadata record.
							break LoopRec;
						}
					}
				}
			}
		}

		//State in console how many of processed entries for this topicCat have identified an Application among their resources.
		console.log(topicApps.length+' '+topicCat+' entries have Applications listed.');

		var loggedApps = [];
		//Loop through entries with Applications and extract fields needed to generate content on Drupal end
		for (var j = 0; j < topicApps.length; j++) {
			
			var recordFormats = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'].length;
			
			for (var k = 0; k < recordFormats; k++) {
				uuid = topicApps[j]['gmd:fileIdentifier']['gco:CharacterString'];
				                                   
				titleEN = '';
				bodyEN = '';
				keysEN = '';
				titleFR = '';
				bodyFR = '';
				keysFR = '';
				
				//Check if the metadata record has descriptive keywords entered
				if(topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']) {
					
					//Handling variations in structure of child tags under ['gmd:descriptiveKeywords']
					//Differences in number of MD_Keywords, whether MD_Keywords tag is parent to more than one keyword, or if only one keyword is used.
					if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'].length) {
						nbrDescKeywords = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'].length;
						//Multiple ['gmd:descriptiveKeywords'] with different structures
						for (var descKeys = 0; descKeys < nbrDescKeywords; descKeys++) {
							if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'].length) {
								nbrKeywords = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'].length;
								//Loop through descriptiveKeywords to create keywords text string
								for (var keys = 0; keys < nbrKeywords; keys++) {
									if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gco:CharacterString']) {
										keysEN += topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gco:CharacterString']+", ";
									}
									if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_']) {
										keysFR += topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword'][keys]['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_']+", ";
									}
								}
							} else {
								keysEN = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword']['gco:CharacterString'];
								keysFR = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords'][descKeys]['gmd:MD_Keywords']['gmd:keyword']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
							}
						}
					//The next block was the original If start. << Delete after debugged.
					} else if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'].length) {
						nbrKeywords = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'].length;
						//Loop through gmd:keyword tags to create keywords text string
						for (var keys = 0; keys < nbrKeywords; keys++) {
							if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'][keys]['gco:CharacterString']) {
								keysEN += topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'][keys]['gco:CharacterString']+", ";
							}
							if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'][keys]['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_']) {
								keysFR += topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'][keys]['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_']+", ";
							}
						}
					} else {
						keysEN = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword']['gco:CharacterString'];
						keysFR = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
					}
					
				}

				if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length) {
					onLineNbr = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length
					for (var onlineOpts = 0; onlineOpts < onLineNbr; onlineOpts++) {
						if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][onlineOpts]['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
							if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][onlineOpts]['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("eng")) {
								titleEN = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][onlineOpts]['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'];
								bodyEN = "<iframe src='"+topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][onlineOpts]['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%'></iframe>";
								for (var fr = 0; fr < recordFormats; fr++) {
									//console.log(chalk.bgRed.white(topicCat+" - "+uuid+" - "+titleEN+" - "+titleFR));
									//console.log(chalk.bgRed.white(fr+" - "+onlineOpts));
									var shortTitle = titleEN.substr(0, titleEN.indexOf('('));
									if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][fr]['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'].includes(shortTitle) && topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][fr]['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("fra")) {
										titleFR = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][fr]['gmd:CI_OnlineResource']['gmd:name']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
										bodyFR = "<iframe src='"+topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][fr]['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%'></iframe>";
									}
								}
							}
						}
					}
				} else {
					if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
						if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("eng")) {
							titleEN = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'];
							bodyEN = "<iframe src='"+topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%'></iframe>";
							for (var fr = 0; fr < recordFormats; fr++) {
								//console.log(chalk.bgRed.white(topicCat+" - "+uuid+" - "+titleEN));
								//console.log(chalk.bgRed.white(fr));
								var shortTitle = titleEN.substr(0, titleEN.indexOf('('));
								if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][fr]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'].includes(shortTitle) && topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][fr]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("fra")) {
									titleFR = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][fr]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:name']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
									bodyFR = "<iframe src='"+topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][fr]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%'></iframe>";
								}
							}
						}
					}
				}
				//Check the array of application titles already appended to SQL table, and skip if record already created.
				if (!loggedApps.includes(titleEN)) {
					loggedApps.push(titleEN);
					
					//*****add a variable and table column to handle what ISO theme the application is classified as*****
					console.log("Have catalogued "+loggedApps.length+" applications."); //For debugging, can remove
					console.log(uuid+"     "+topicCat+"   "+thumb+"   "+titleEN+"   "+titleFR+"   "+bodyEN+"   "+bodyFR+"   "+keysEN+"   "+keysFR); //For debugging, can remove
					console.log(chalk.bgBlue.white(topicCat, "          ", titleEN));
					//Check AWS MySQL DB to see if record for the same uuid and Application name already exists.
					//console.log(chalk.bgGreen.white(topicCat));
					awsDBcheck(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat, function(result, uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat) {
						//console.log(chalk.bgRed.white(topicCat));
						//console.log("result is "+result);
						//console.log(uuid);
						//If the FGP record's application resource does not already exist in AWS db, add the FGP record as Drupal content.
						if (result == "exists") {
							//Skip this record if "exists" returns true from checking the AWS database.
						} else if (result == "new") {
							//console.log("in new result "+uuid+" - "+titleEN+" - "+topicCat);
							
							try {
								awsDBwrite(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR, topicCat);
								contentProcessed++;
								//console.log(chalk.bgGreen.white(contentProcessed+" entries added to DB."));
							}
							catch(err) {
								console.log("Error in trying to insert record to AWS database.");
								console.log(err);
							}
						}
					});
				} else {		
					//console.log("Skipping "+titleEN+"!"); //For debugging, can remove
					continue;
				}			
			}
		}
	});
}

//awsDBconnect
//	.then(
topicCats.forEach(async function (topicCat) {
	const args = {
		parameters: {
			topicCat: topicCat,
			fast: "false"
		},
		headers: { "Content-Type": "application/json" }
	};

	gnQuery(args, topicCat);
})
//	)

//Still need to:
// 1 - Loop through topicCats list and add content for each as the right ISO content category
// 2 - Combine topicCats into the right theme as it will be presented in Web Presence
// 3 - Test for multiple topicCats (only farming being done right now).
// 4 - Figure out Drupal Migrate to connect Drupal site to AWS DB that will hold featured application content pages.
