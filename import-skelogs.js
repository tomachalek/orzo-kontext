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
    "userMapPath": [path to a json or mysql connection URI]
 * }
 *
 */

/// <ref path="./libs/orzojs.d.ts" />

var applog = require('applog');
var apachelog = require('apachelog');
var rec2elastic = require('rec2elastic');
var proc = require('processing');
var conf = proc.validateConf(orzo.readJsonFile(env.inputArgs[0]));
if (!conf['userMapPath']) {
    throw new Error('missing userMapPath configuration');
}
var worklog = new proc.Worklog(conf['workLogPath'],
        new Date(env.startTimestamp * 1000), 86400);
var parseLine = apachelog.createParser(
    apachelog.lineParsers.parseSkeLine,
    apachelog.dateParsers.parseYMDDatetime,
    '/ske/run.cgi',
    getUserMap(conf['userMapPath'])
);
var dryRun = getAttr(conf, 'dryRun', true);


function getUserMap(src) {
    var db, rows, row, ans;

    if (src.indexOf('mysql') === 0) {
        ans = {};
        db = orzo.db.connect(src);
        rows = db.select('SELECT user, id FROM user');
        while (rows.hasNext()) {
            row = rows.next();
            ans[row[0]] = row[1];
        }
        return ans;

    } else {
        return orzo.readJsonFile(src)
    }
}


function gzipFileInRange(path) {
    var lastLine = null;
    var parsed;
    doWith(orzo.gzipFileReader(path), function (fr) {
        while (fr.hasNext()) {
            lastLine = fr.next();
        }
    });
    parsed = parseLine(lastLine);
    if (parsed && parsed.isOK()) {
        if (parsed.getDate().getTime() / 1000 < worklog.getLatestTimestamp()) {
            return false;

        } else {
            return true;
        }
    }
    return false;
}

function fileIsInRange(path) {
    if (path.indexOf('.gz') === path.length - 3) {
        return gzipFileInRange(path);

    } else {
        return proc.fileIsInRange(path, worklog.getLatestTimestamp(), parseLine);
    }
}

function isInRange(item) {
    return item.getDate().getTime() / 1000 >= worklog.getLatestTimestamp();
}


dataChunks(getAttr(conf, 'numApplyWorkers', 1), function (idx) {
    return orzo.directoryReader(conf.srcDir, idx);
});


processChunk(function (filePaths, map) {
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


map(function (item) {
    var reader;

    if (item.indexOf('.gz') === item.length - 3) {
        reader = orzo.gzipFileReader(item);

    } else {
        reader = orzo.fileReader(item);
    }
    doWith(
        reader,
        function (fr) {
            var parsed,
                converted;

            while (fr.hasNext()) {
                parsed = parseLine(fr.next());
                if (parsed && parsed.isOK() && isInRange(parsed) && applog.agentIsHuman(parsed)) {
                    converted = rec2elastic.convertRecord(parsed, 'ske');
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

