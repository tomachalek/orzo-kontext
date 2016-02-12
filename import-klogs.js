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

/// <ref path="./libs/orzojs.d.ts" />

var applog = require('applog');
var rec2elastic = require('rec2elastic');
var proc = require('processing');
var conf = proc.validateConf(orzo.readJsonFile(env.inputArgs[0]));
var worklog = new proc.Worklog(conf['workLogPath'],
        new Date(env.startTimestamp * 1000), getAttr(conf, 'defaultCheckInterval', 86400));
var dryRun = getAttr(conf, 'dryRun', true);
var printInserts = getAttr(conf, 'printInserts', false);
var anonymousId = getAttr(conf, 'anonymousUserId', 0);

var ip2geo = orzo.createIp2Geo();

function getGeoData(ipAddress) {
    var data;

    try {
        data = ip2geo(ipAddress);

    } catch (e) {
        orzo.print('ignoring unknown IP ' + ipAddress);
        data = {};
    }
    return data;
}


dataChunks(getAttr(conf, 'numApplyWorkers', 1), function (idx) {
    return orzo.directoryReader(conf.srcDir, idx);
});


processChunk(function (filePaths, map) {
    var currPath;

    while (filePaths.hasNext()) {
        currPath = filePaths.next();
        if (!proc.fileIsInRange(currPath, worklog.getLatestTimestamp(), applog.parseLine)) {
            if (!dryRun) {
                try {
                    //orzo.fs.moveFile(currPath, conf.archivePath);

                } catch (e) {
                    map({'error': e});
                }
            }

        } else {
            map(currPath);
        }
    }
});


function isInRange(item) {
    return item.getDate().getTime() / 1000 >= worklog.getLatestTimestamp();
}

function isValid(item) {
    return item
            && item.isOK()
            && item.getType() === 'INFO'
            && isInRange(item)
            && applog.agentIsHuman(item)
}

map(function (item) {
    if (typeof item === 'object' && item['error']) {
        emit('error', item['error']);

    } else {
        doWith(
            orzo.fileReader(item),
            function (fr) {
                var line,
                    parsed,
                    converted;

                while (fr.hasNext()) {
                    line = fr.next();
                    parsed = applog.parseLine(line);
                    if (isValid(parsed)) {
                        parsed.setDateFormat(getAttr(conf, 'dateFormat', 0));
                        converted = rec2elastic.convertRecord(parsed, 'kontext',
                            anonymousId, getGeoData(parsed.getRemoteAddr()));
                        emit('result', [converted.metadata, converted.data]);
                    }
                }
            },
            function (err) {
                orzo.print('error: ' + err);
            }
        );
    }
});


reduce(getAttr(conf, 'numReduceWorkers', 1), function (key, values) {
    if (key === 'result') {
        var bulkInsert = new rec2elastic.BulkInsert(conf.bulkUrl, conf.chunkSize, dryRun);
        bulkInsert.setPrintInserts(printInserts);
        bulkInsert.insertValues(values);
        emit('result', values.length);

    } else if (key === 'error') {
        emit('errors', values);
    }
});


finish(function (results) {
    var report = [orzo.sprintf('Inserted %s records', results.get('result')[0])];
    results.get('errors').slice(0, 10).forEach(function (item) {
        report.push(item.toString());
    });

    if (!dryRun) {
        worklog.close();
    }

    return JSON.stringify(report);
});
