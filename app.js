require.paths.unshift(__dirname + '/modules', __dirname + '/lib/node', __dirname);

var simpledb = require('simpledb'),
    fs = require('fs'),
    lr = require('linereader'),
    argv = require('optimist').argv,
    request = require('request');

if (!argv.config) {
    console.log("Must provide --config argument which points to json settings file, such as --config settings.json");
    process.exit(1);
}

var options = {};
try {
    var config = JSON.parse(fs.readFileSync(argv.config, 'utf8'));
    for (var key in config) {
        options[key] = config[key];
    }
} catch(e) {
   console.warn('Invalid JSON config file: ' + options.config);
   throw e;
}

// Grab certain arguments from CLI if not in settings.json
options['backupTo'] = options['backupTo'] || argv.backupTo;
options['restore'] = options['restore'] || argv.restore;
options['migrate'] = options['migrate'] || argv.migrate;

if (!options.migrate) {
    // Connect to SimpleDB.
    var sdb = new simpledb.SimpleDB({keyid:options.awsKey,secret:options.awsSecret});
}

// Restore a database.
if (options.restore) {
    // Check if domain exists.  If not, create it.  If exists, exit.
    sdb.domainMetadata(options.restoreToDomain, function(err, res, meta) {
        // For now, only support restoring to new domain.
        if (err && err.Code == "NoSuchDomain") {
            // Create the domain.  Then restore.
            sdb.createDomain(options.restoreToDomain, function(err, res, meta) {
                if (!err) {
                    try {
                        reader = new lr.linereader(options.restoreFrom, 1024);
                        while (reader.hasNextLine()) {
                            var item = JSON.parse(reader.nextLine());
                            var itemName = item.$ItemName;
                            delete item.$ItemName;
                            console.log(itemName);
                            sdb.putItem(options.restoreToDomain, itemName, item, function(err, res, meta) {
                                // TODO: logging.
                            });
                        }
                    } catch (err) {
                        console.log("Error reading file.  Error was: " + err);
                    } 
                }
            });
        }
        else {
            console.log("Domain exists.  Please specify a domain to restore to which does not yet exist");
            process.exit(1);
        }
    });
}

else if (options.migrate) {
    try {
        reader = new lr.linereader(options.restoreFrom, 1024);
        while (reader.hasNextLine()) {
            var doc = {};
            var item = JSON.parse(reader.nextLine());
            var itemName = item.$ItemName;
            delete item.$ItemName;
            for (var i in item) {
                try {
                    item[i] = JSON.parse(item[i]);
                } catch (e) { console.log(item[i]) }
                finally {
                    doc[i] = item[i];
                }
            }

            request.put({
              uri: 'http://localhost:5984/' + options.couchdbTarget + '/' + encodeURIComponent(itemName),
              headers: {'Content-Type': 'application/json'},
              json: doc
            }, function (err, res, body) {
                   // TODO: error reporting.
               }
            );

        }
    } 
    catch (err) {
        console.log("Error reading file.  Error was: " + err);
    } 
}

// Backup a database.
else {
    try {
        // Delete existing backup file.  Otherwise, backups append to existing backups.
        if (fs.statSync(options.backupTo)) {
            fs.writeFileSync(options.backupTo, "");
        }
    } catch (err){};
    var file = fs.openSync(options.backupTo, 'a');
    // Get item names first, then get each item. "select" has a 1MB result
    // therefore we're less likely to hit that limit by getting each
    // individual item.
    sdb.select("SELECT $ItemName FROM " + options.backupFromDomain, function(err, res, metadata) {
        if (res) {
            res.forEach(function(item) {
                var obj = {};
                sdb.getItem(options.backupFromDomain, item["$ItemName"], function(error, row, rowMeta) {
                    fs.writeSync(file, JSON.stringify(row) + "\n");
                });
            });
        }
    });

}
