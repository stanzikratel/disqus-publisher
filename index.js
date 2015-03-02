var _ = require('underscore');
var pretty = require('pretty-data').pd;
var es = require('elasticsearch');
var disqusExportParser = require('disqus-export-parser');


exports.cli = function () {
	// get the parameters
	var filename = process.argv[2];
	var searchServerHost = process.argv[3];
	var searchServerPort = process.argv[4];
	var searchIndex = process.argv[5];
	console.log("Parameters: %s", filename, searchServerHost, searchServerPort, searchIndex);

	// Call publish
};

exports.publish = function (inputFileName, searchServerHost, searchServerPort, searchIndex) {
}


exports.cli();
