const Client = require('node-rest-client').Client;
const chalk = require('chalk');
const mysql = require('mysql');
const fs = require('fs');
const forEP = require('foreach-promise');
const connxnFile = './awsconnect.json';

//Declare a new Client object for handling REST requests through Node
var client = new Client();

//Set up an object for holding all GeoNetwork records for the current topic that have an Application
var topicApps = [];
//var args;
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

//Construct REST call, and write to AWS content db based on checks to database to ensure content isn't duplicated.
async function gnQuery(args, topicCat) {
		topicApps = [];
		
		client.post("https://maps-dev.canada.ca/geonetwork/srv/csw/q", args, function (data, response) {
			//Select from response all metadata record objects and determine how many records exist in this topic category
			var gnRecords = data.response['gmd:MD_Metadata'];
			var nbrGNRecords = gnRecords.length;
			console.log(chalk.bgGreen.white("topicCat is currently "+topicCat+". There are "+nbrGNRecords+" records returned."));
			//Loop through records returned for this category and filter to only "Applications".
			for ( var i = 0; i < nbrGNRecords; i++) {
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
			console.log(chalk.bgGreen.white(topicApps.length+' '+topicCat+' entries have Applications listed.'));	
			
			var loggedApps = [];
	
			//Loop through entries that have Applications listed and extract fields needed to generate content on Drupal end.
			for (var j = 0; j < topicApps.length; j++) {
				var recordFormats = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'].length;
				var ind
				for (var k = 0; k < recordFormats; k++) {
					uuid = '';
					thumb = '';
					titleEN = '';
					bodyEN = '';
					keysEN = '';
					titleFR = '';
					bodyFR = '';
					keysFR = '';
					//Handle variance in ['gmd:onLine'] tags
					onlineOptions = topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'].length;
					if (!onlineOptions) {
						
					} else {
						ind = onlineOptions
					}
					console.log(topicApps[j]['gmd:distributionInfo']['gmd:MD_Distribution']['gmd:transferOptions'][k]['gmd:MD_DigitalTransferOptions']['gmd:onLine'][ind]);
					
				}		
			}
		});
}

const args = {
	parameters: {
		topicCat: "farming",
		fast: "false"
	},
	headers: { "Content-Type": "application/json" }
};
topicCat = "farming";

gnQuery(args, topicCat);