/*
 * Extract start and finish dates
 *
 * config:
 * {
    "srcDir": ...,
    "archivePath": ...,
    "bulkUrl": ...,
    "chunkSize": ...,
    "defaultCheckInterval": ...,
    "workLogPath": ...,
    "numApplyWorkers": ...,
    "numReduceWorkers": ...,
    "dryRun": true/false
 * }
 *
 */

/// <ref path="./orzojs.d.ts" />

var applog = require('applog');
var rec2elastic = require('rec2elastic');
var conf = validateConf(orzo.readJsonFile(env.inputArgs[0]));
var proc = require('processing');
var worklog = new proc.Worklog(conf['workLogPath'],
        new Date(env.startTimestamp * 1000), 86400);
var dryRun = getAttr(conf, 'dryRun', true);


function validateConf(c) {
    var props = ['srcDir', 'archivePath', 'bulkUrl', 'chunkSize', 'defaultCheckInterval',
                 'workLogPath'];
    props.forEach(function (item) {
       if (c[item] === undefined) {
           throw new Error(orzo.sprintf('Missing configuration item "%s"', item));
       }
    });
    return c;
}

function fileIsInRange(filePath) {
    var reader = orzo.reversedFileReader(filePath);
    var lastLine = null;
    var parsed = null;
    var ans = false;
    while (reader.hasNext()) {
        lastLine = reader.next();
        if (lastLine) {
            parsed = applog.parseLine(lastLine);
            if (parsed && parsed.isOK()) {
                if (parsed.getDate().getTime() / 1000 < worklog.getLatestTimestamp()) {
                    break;

                } else {
                    ans = true;
                    break;
                }
            }
        }
    }
    reader.close();
    return ans;
}


dataChunks(getAttr(conf, 'numApplyWorkers', 1), function (idx) {
    return orzo.directoryReader(conf.srcDir, idx);
});


applyItems(function (filePaths, map) {
    var currPath;

    while (filePaths.hasNext()) {
        currPath = filePaths.next();

        if (!fileIsInRange(currPath)) {
            orzo.fs.moveFile(currPath, conf.archivePath);

        } else {
            map(currPath);
        }
    }
});


function isInRange(item) {
    return item.getDate().getTime() / 1000 >= worklog.getLatestTimestamp();
}

function isHuman(parsed) {
    return !applog.agentIsMonitor(parsed.getUserAgent())
            && !applog.agentIsBot(parsed.getUserAgent());
}


map(function (item) {
    doWith(
        orzo.fileReader(item),
        function (fr) {
            var parsed,
                converted;

            while (fr.hasNext()) {
                parsed = applog.parseLine(fr.next());
                if (parsed && parsed.isOK() && isInRange(parsed) && isHuman(parsed)) {
                    converted = rec2elastic.convertRecord(parsed, 'kontext');
                    emit('result', [converted.metadata, converted.data]);
                }
            }
        },
        function (err) {
            orzo.print('error: ' + err);
        }
    );

});


reduce(getAttr(conf, 'numReduceWorkers', 1), function (key, values) {
    if (key === 'result') {
        var bulkInsert = new rec2elastic.BulkInsert(conf.bulkUrl, conf.chunkSize, dryRun);
        bulkInsert.setPrintInserts(true);
        bulkInsert.insertValues(values);
    }
});


finish(function (results) {
    worklog.close();
});