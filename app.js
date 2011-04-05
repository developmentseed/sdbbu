require.paths.unshift(__dirname + '/modules', __dirname + '/lib/node', __dirname);

var simpledb = require('simpledb'),
    fs = require('fs'),
    lr = require('linereader');

var sdb = new simpledb.SimpleDB({keyid:'AWS_KEY',secret:'AWS_SECRET'});
var domain = process.argv[2];
var output = process.argv[3];

if (process.argv[4] == "restore") {
    try {
        reader = new lr.linereader(output, 1024);
        while (reader.hasNextLine()) {
            var line = reader.nextLine();
            console.log(JSON.parse(line));
            // @TODO batch put in groups of X items into domain
            // or just put one by one for now.
        }
    } catch (err) {
        console.log("Error reading file.  Error was: " + err);
    }
}

else {
    var file = fs.openSync(output, 'a');
    // Get item names first, then get each item. "select" has a 1MB result
    // therefore we're less likely to hit that limit by getting each
    // individual item.
    sdb.select("SELECT $ItemName FROM " + domain, function(err, res, metadata) {
        if (res) {
            res.forEach(function(item) {
                var obj = {};
                sdb.getItem(domain, item["$ItemName"], function(error, row, rowMeta) {
                    fs.writeSync(file, JSON.stringify(row) + "\n");
                });
            });
        }
    });

}
