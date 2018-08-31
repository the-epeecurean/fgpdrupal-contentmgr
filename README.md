# Content Manager for FGP Web Presence

Connects to public-facing REST enspoint for Federal Geospatial Platform metadata and scrapes entries for applications. Metadata needed to create website pages in Drupal Web Presence instance are scraped and saved to an external database.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project.

If you are going to deploy on AWS Lambda as a service executable, will need to package program scripts with node_modules folder in a ZIP file.

As the required modules are not included in the repo, you will need to locally install node and run 

```
npm install "module"
```

for all packages before bundling the compressed project file for Lambda.

### Prerequisites

What things you need to install the software and how to install them

Aside from core Node.js modules you will need:

* [node-rest-client] (https://www.npmjs.com/package/node-rest-client) - The client used to construct REST queries
* [chalk] (https://www.npmjs.com/package/chalk) - Used to stylize reporting and errors logged to console during runtime
* [mysql] (https://www.npmjs.com/package/mysql) - Node.js driver to make mysql requests
* [foreach-promise] (https://www.npmjs.com/package/foreach-promise) - Used to handle nested forEach loop through array of topicCat used by FGP as one big promise for query handling

## Authors

* **Jay Logan** - *Initial work*
