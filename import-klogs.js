/*
 * Extract start and finish dates
 *
 * config:
 * {
 *   "srcDir": "...",
 *   "bulkUrl": "...",
 *   "chunkSize": 50,
 *   "defaultCheckInterval": 86400,
 *   "workLogPath": "...",
 *   "numApplyWorkers": 4,
 *   "numReduceWorkers": 1
 * }
 *
 */

/// <ref path="./orzojs.d.ts" />

var applog = require('applog');
var rec2elastic = require('rec2elastic');
var wl = require('worklog');

var conf = orzo.readJsonFile(env.inputArgs[0]);
var worklog = new wl.Worklog(conf['tweets']['worklogPath'],
        new Date(env.startTimestamp * 1000), 86400);


function containsAll(str) {
    var srch = Array.prototype.slice.call(arguments, 1);
    return srch.reduce(function (prev, curr) {
        return prev && str.indexOf(curr) > -1;
    }, true);
}


function agentIsMonitor(agentStr) {
    /*
    Python-urllib/2.7
    Zabbix-test
    */
    agentStr = agentStr ? agentStr.toLowerCase() : '';
    return containsAll(agentStr, 'python-urllib/2.7')
        || containsAll(agentStr, 'zabbix-test');
}

/**
 *
 */
function agentIsBot(agentStr) {
    /*
    Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
    Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)
    Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)
    Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)
    Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)
    Mozilla/5.0 (compatible; SeznamBot/3.2; +http://fulltext.sblog.cz/)
    Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)
    Mozilla/5.0 (compatible; MegaIndex.ru/2.0; +http://megaindex.com/crawler)
    */
    agentStr = agentStr ? agentStr.toLowerCase() : '';
    return containsAll(agentStr, 'googlebot')
        || containsAll(agentStr, 'ahrefsbot')
        || containsAll(agentStr, 'yandexbot')
        || containsAll(agentStr, 'yahoo', 'slurp')
        || containsAll(agentStr, 'baiduspider')
        || containsAll(agentStr, 'seznambot')
        || containsAll(agentStr, 'bingbot')
        || containsAll(agentStr, 'megaindex.ru');
}


function getFileRange(fileReader) {
    var first = null,
        testLast = null,
        last = null,
        line;

    while ((!first || !first.isOK()) && fileReader.hasNext()) {
        first = applog.parseLine(fileReader.next());
    }

    if (!first || first && first.getDate().getTime() > TO_DATE.getTime()) {
        orzo.print('ignoring (quickly): ' + fileReader.path);
        return null;
    }

    while (fileReader.hasNext()) {
        testLast = applog.parseLine(fileReader.next());
        if (testLast && testLast.isOK()) {
            last = testLast;
        }
    }
    if (!last) {
        last = first;
    }

    if (last && last.getDate().getTime() < FROM_DATE.getTime()) {
        orzo.print('ignoring: ' + fileReader.path);
        return null;
    }

    return fileReader.path;
}


function rangeMatch(from, to) {
    if (to.getTime() < FROM_DATE.getTime() || from.getTime() > TO_DATE.getTime()) {
        return false;
    }
    return true;
}


dataChunks(conf.numApplyWorkers, function (idx) {
    return orzo.directoryReader(conf.srcDir, idx);
});


applyItems(function (filePaths, map) {
    while (filePaths.hasNext()) {
        doWith(
            orzo.fileReader(filePaths.next()),
            function (fr) {
                var ans = getFileRange(fr);
                if (ans) {
                    map(fr.path);
                }
            },
            function (err) {
                orzo.print('error: ' + err);
            }
        );
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
                if (parsed
                        && parsed.isOK()
                        && isInRange(parsed)
                        && !agentIsMonitor(parsed.getUserAgent())
                        && !agentIsBot(parsed.getUserAgent())) {

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


reduce(conf.numReduceWorkers, function (key, values) {
    if (key === 'result') {
        var bulkInsert = new rec2elastic.BulkInsert(conf.bulkUrl, conf.chunkSize, true);
        bulkInsert.insertValues(values);
    }
});


finish(function (results) {
    worklog.close();
});