// Declare a new Client object, requiring the node-rest-client NodeJS package
var Client = require('node-rest-client').Client;
var client = new Client();

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
uuid = "4b1d45b0-5bfe-4c6d-bcd3-96c9d821ad3b";
titleEN = "AAFC Crop Inventory (English)";
topicCat = "farming";

function awsQuery() {
	con.query("SELECT * FROM fgpwp_content WHERE uuid='"+uuid+"' AND titleEN='"+titleEN+"' AND topic='"+topicCat+"'", function (error, results, fields) {
		console.log(results);
		if(results != '') {
			console.log("True!");
		} else {
			console.log("False!");
		}
		if (error) throw error;
		
	});
}

function awsDBdisconnect() {
	//Disconnect from the AWS database
	con.end();
	console.log("Database connection dropped!");
	
}

awsDBconnect();
awsQuery();
awsDBdisconnect();