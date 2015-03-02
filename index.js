var _ = require('underscore');
var pretty = require('pretty-data').pd;
var es = require('elasticsearch');
var disqusExportParser = require('disqus-export-parser');


var addDocumentToIndex = function (client, index, type, id, body, context, cbJob) {
	try {
		client.index({
			index: index,
			requestTimeout: 300000,
			type: type,
			id: id,
			body: body
		}, function (error, response) {
			context.processed++;
			if (error) {
				context.errors++;
				console.log("*** ERROR indexing %s: %s", id, error);
			}
			else {
				context.success++;
				console.log("%d: Indexed %s", context.processed, body._id);
			}

			if (context.processed >= context.targetNumber) {
				cbJob(null, context);
			}
		});
	}
	catch (e) {
		console.log("Error in indexing a document." + e);
	}
};
var addDocumentsToIndex = function (client, searchIndex, searchType, docs, cbJob) {
	var context = {processed: 0, success: 0, errors: 0, targetNumber: (docs||[]).length };
	if (_.isArray(docs) && docs.length) {

		_.each(docs, function (doc, i) {
			console.log(doc);
			addDocumentToIndex(client, searchIndex, searchType, doc._id, doc, context, cbJob);
			console.log("%d: Processing %s", i + 1, doc._id);
		});
	}
	else { cbJob("Invalid documents collection sent to addDocumentsToIndex()", context); }
};




exports.publish = function (inputFilename, searchServerHost, searchServerPort, searchIndex, cb) {
        console.log("Parameters: %s", inputFilename, searchServerHost, searchServerPort, searchIndex);

	disqusExportParser.parse(inputFilename, "object", function(err, buffer) {
		if (err) {
			console.log("\n\nError during file parse.");
			console.dir(err);
			if (cb) { cb(err, {}); }
			return;
		}

		// Step 1: Create the ElasticSearch client
		var client = new es.Client({host: searchServerHost + ':' + searchServerPort, log: 'error'});

		// Step 2: Collect the set of Disqus Posts to index and make it ES-friendly
		var docs = _.map(buffer.disqus.post, function(post) { post._id = post.a["dsq:id"]; return post; });

		debugger;

		// Step 3: Call AddDocumentsToIndex() with the proper child property and wait for cb
		addDocumentsToIndex(client, searchIndex, "post", docs, function(err, ctx) {
			cb(err, ctx);
		});
	});
};
exports.cli = function () {
	// get the parameters
	var filename = process.argv[2];
	var searchServerHost = process.argv[3] || "127.0.0.1";
	var searchServerPort = process.argv[4] || "9200";
	var searchIndex = process.argv[5] || "disqus";
	if (process.argv.length < 3) {
		console.log("\n\nUsage: bin/disqus-publisher <export_filename> <es_server> <es_port> <es_index>\n\n");
		process.exit(1);
	}

	// Call publish
	exports.publish(filename, searchServerHost, searchServerPort, searchIndex,
		function(err, ctx) {
			if (err) {
				console.log("Error publishing...");
				console.dir(err);
			}
			else {
				console.log("\n\n");
				console.dir(ctx);
			}
			console.log("\n\n");
			process.exit(0);
		});
};
