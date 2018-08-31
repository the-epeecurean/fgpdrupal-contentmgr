// Declare a new Client object, requiring the node-rest-client NodeJS package
var Client = require('node-rest-client').Client;
var client = new Client();

//Array for all Topic Categories that will be requested from the GeoNetwork REST
var topicCats = [
	'biota',
	'boundaries',
	'climatology, meteorology, atmosphere',
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
	'planning cadastre',
	'society',
	'structure',
	'transportation',
	'utilities communication'	
];
//Set up an object for holding all GeoNetwork records for the current topic that have an Application
var topicApps = [];
//Variables to hold entry fields to output to Drupal content
var uuid;
var thumb;
var titleEN;
var bodyEN;
var keysEN;
var titleFR;
var bodyFR;
var keysFR;

//Use mysql package and connect to AWS DB
var mysql = require('mysql');
var con;
function awsDBconnect() {
	con = mysql.createConnection({
		host: "fgp-drupal-02.cc0hbqeyyxxw.ca-central-1.rds.amazonaws.com",
		user: "admin",
		password: "password",
		database: "gnFGPentries"
	});
	
	con.connect(function(err) {
		if (err) throw err;
		console.log("Connected to database!");
	});
}

function awsDBwrite(uuid, thumb, titleEN, bodyEN, keysEN, titleFR, bodyFR, keysFR) {
	//Insert a new content table row for the FGP entry
	//conn.query('INSERT INTO fgpwpcontent (uuid, thumbnailURL, titleEN, bodyEN, keywordsEN, titleFR, bodyFR, keywordsFR) VALUES ()'
}

function awsDBdisconnect() {
	//Disconnect from the AWS database
	con.end();
	console.log("Database connection dropped!");
}	

//Set content-type header to request response as JSON type
//***WILL want to loop through the POST request by using the array of values for topicCat...***
var args = { 
	parameters: { 
		// uuid,
		topicCat: "farming",
		fast: "false"
	},
	headers: { "Content-Type": "application/json" }
};

awsDBconnect();

client.post("https://maps-dev.canada.ca/geonetwork/srv/csw/q", args, function (data, response) {
	//parsed response body as JS object
	//console.log(data);
	
	//Select from response all metadata record objects and determine how many records exist in this topic category
	var gnRecords = data.response['gmd:MD_Metadata'];
	var nbrGNRecords = gnRecords.length;

	//Loop through records returned for this category and filter to only "Applications".
	for ( var i = 0; i < nbrGNRecords; i++) {
		console.log(i+" - "+gnRecords[i]['gmd:fileIdentifier']['gco:CharacterString']);
		
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

	console.log(topicApps.length+/*' '+topicCat+*/' farming'+' entries have Applications listed.');

	var loggedApps = [];
	//Loop through entries with Applications and extract fields needed to generate content on Drupal end
	for (var j = 0; j < topicApps.length; j++) {
		
		var recordFormats = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'].length;
		
		for (var k = 0; k < recordFormats; k++) {
			uuid = '';
			thumb = '';
			titleEN = '';
			bodyEN = '';
			keysEN = '';
			titleFR = '';
			bodyFR = '';
			keysFR = '';
			if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("Application")) {
				if (topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:description']['gco:CharacterString'].includes("eng")) {
					uuid = topicApps[j]['gmd:fileIdentifier']['gco:CharacterString'];
					thumb = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:graphicOverview']['gmd:MD_BrowseGraphic']['gmd:fileName']['gco:CharacterString'];
					titleEN = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:name']['gco:CharacterString'];

					//Check the array of application titles already appended to SQL table, and skip if record already created.
					if (!loggedApps.includes(titleEN)) {
						bodyEN = "<iframe src='"+topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%''></iframe>";
						try {
							titleFR = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k+1]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:name']['gmd:PT_FreeText']['gmd:textGroup']['gmd:LocalisedCharacterString']['_'];
							bodyFR = "<iframe src='"+topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k+1]['gmd:MD_DigitalTransferOptions']['gmd:onLine']['gmd:CI_OnlineResource']['gmd:linkage']['gmd:URL']+"' height='100%' width='100%''></iframe>";
						}
						catch(err) {
							console.log(uuid+"     "+titleEN+"          "+k);
							console.log(err);
						}
						
						//Check if the metadata record has descriptive keywords entered
						if(topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']) {
							//------->> Having issues at ['gmd:descriptiveKeywords']['gmd:MD_Keywords'] ... ['gmd:descriptiveKeywords'] appears undefined on some records
							//console.log(j +" - "+k+" - "+uuid+" - "+Object.getOwnPropertyNames(topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']));
							nbrKeywords = topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'].length; //['gmd:MD_Keywords']
							
							//Check whether the MD_Keywords tag is parent to more than one keyword, or if only one keyword is used.
							if (topicApps[j]['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:descriptiveKeywords']['gmd:MD_Keywords']['gmd:keyword'].length) {
								//Loop through descriptiveKeywords to create keywords text string
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
						//grab french options from ['gmd:transferOptions'][k+1] (French translation links are often directly after English in metadata profile list)
						//iterate past [k+1] (otherwise french will have content translation AND separate content page)
						//add a variable and table column to handle what ISO theme the application is classified as
						loggedApps.push(titleEN);
						console.log("Have catalogued "+loggedApps.length+" applications already.");
						console.log(loggedApps);
						//console.log(uuid+"   "+thumb+"   "+titleEN+"   "+titleFR+"   "+bodyEN+"   "+bodyFR+"   "+keysEN+"   "+keysFR);
					} else {
						
						console.log("Skipping "+titleEN+"!");
						continue;
					}
				}
			}
		}
	}
	
	awsDBdisconnect();
	
});


//Still need to:
// 1 - Loop through topicCats list and add content for each as the right ISO content category
// 2 - Combine topicCats into the right theme as it will be presented in Web Pres
// 3 - Construct query that will write content record to AWS database
// 3a - Test for multiple topicCats (only farming being done right now).
// 4 - Figure out Drupal Migrate to connect Drupal site to AWS DB that will hold featured application content pages.
