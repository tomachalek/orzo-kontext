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
    "dryRun": true/false,
    "dateFormat": 0/1
 * }
 *
 */

/// <ref path="./orzojs.d.ts" />

var applog = require('applog');
var rec2elastic = require('rec2elastic');
var proc = require('processing');
var conf = proc.validateConf(orzo.readJsonFile(env.inputArgs[0]));
var worklog = new proc.Worklog(conf['workLogPath'],
        new Date(env.startTimestamp * 1000), 86400);
var dryRun = getAttr(conf, 'dryRun', true);


dataChunks(getAttr(conf, 'numApplyWorkers', 1), function (idx) {
    return orzo.directoryReader(conf.srcDir, idx);
});


applyItems(function (filePaths, map) {
    var currPath;

    while (filePaths.hasNext()) {
        currPath = filePaths.next();

        if (!proc.fileIsInRange(currPath, worklog.getLatestTimestamp(), applog.parseLine)) {
            if (!dryRun) {
                orzo.fs.moveFile(currPath, conf.archivePath);
            }

        } else {
            map(currPath);
        }
    }
});


function isInRange(item) {
    return item.getDate().getTime() / 1000 >= worklog.getLatestTimestamp();
}


map(function (item) {
    doWith(
        orzo.fileReader(item),
        function (fr) {
            var parsed,
                converted;

            while (fr.hasNext()) {
                parsed = applog.parseLine(fr.next());
                if (parsed && parsed.isOK() && isInRange(parsed) && applog.agentIsHuman(parsed)) {
                    parsed.setDateFormat(getAttr(conf, 'dateFormat', 0));
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