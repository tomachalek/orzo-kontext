/*
 * Merge Apache and Application logs
 *
 * Notes:
 * 1) Czech time: standard: GMT+1, summer: GMT+2
 * 2) Applog does not include timezone info
 *   - standard time in 2015: until 2015-03-29 01:00:00 GMT
 *   - summer time in 2015: until 2015-10-25 01:00:00 GMT
 */

/// <ref path="./orzojs.d.ts" />

var apacheParser = require('apachelog');
var applog = require('applog');

var parseApacheLine = apacheParser.createParser(apacheParser.lineParsers.parseLine,
        apacheParser.dateParsers.parseDMYDatetime);

var APPLOG_FILE = '/home/tomas/work/data/kontext-logs/test/app-clean.log';
var APACHE_LOG_FILE = '/home/tomas/work/data/kontext-logs/test/apache-clean.log';
var OUTPUT_FILE = '/home/tomas/work/orzo-kontext/output.txt';


function itemPasses(item) {
    var userAgent = item.getUserAgent();
    return !(userAgent && applog.agentIsMonitor(userAgent))
            && getAction(item) !== 'nop';
}

function getAction(item) {
    if (item.data) {
        return item.data['action'];
    }
    return null;
}

function getCorpname(item) {
    if (item.data && item.data['params']) {
        return item.data['params']['corpname'];
    }
    return null;
}

function mismatchInIp(item1, item2) {
    return (item1.getRemoteAddr() && item2.getRemoteAddr()
            && item1.getRemoteAddr() !== item2.getRemoteAddr());
}

function mismatchInAction(item1, item2) {
    var a1 = getAction(item1),
        a2 = getAction(item2);
    return a1 && a2 && a1 !== a2;
}


function mismatchInAgent(item1, item2) {
    var a1 = item1.getUserAgent(),
        a2 = item2.getUserAgent();
    return a1 && a2  && a1 !== a2;
}


function mismatchInCorpname(item1, item2) {
    var c1 = getCorpname(item1),
        c2 = getCorpname(item2);
    return c1 && c2 && c1 !== c2;
}

function fetchUserId(item1, item2) {
    if (item1.data && item1.data['user_id']) {
        return item1.data['user_id'];

    } else if (item2.data && item2.data['user_id']) {
        return item2.data['user_id'];
    }
    return null; // this should not happen
}

function fetchAction(item1, item2) {
    if (item1.data && item1.data['action']) {
        return item1.data['action'];

    } else if (item2.data && item2.data['action']) {
        return item2.data['action'];
    }
    return null; // this should not happen
}

function fetchProcTime(item1, item2) {
    if (item1.data && item1.data['proc_time']) {
        return item1.data['proc_time'];

    } else if (item2.data && item2.data['proc_time']) {
        return item2.data['proc_time'];
    }
    return null;
}

function fetchCorpname(item1, item2) {
    var corpname = null,
        limited = null;

    if (item1.data && item1.data['params']) {
        corpname = item1.data['params']['corpname'] || null;
        if (typeof corpname === 'string') {
            corpname = decodeURIComponent(corpname);
            corpname = corpname.split(';')[0];
            if (corpname.indexOf('omezeni/') === 0) {
                corpname = corpname.substr('omezeni/'.length);
                limited = true;

            } else {
                limited = false;
            }
        }
    }
    return [corpname, limited];
}

function fetchIp(item1, item2) {
    if (item1.getRemoteAddr()) {
        return item1.getRemoteAddr();
    }
    return item2.getRemoteAddr();
}

function fetchUserAgent(item1, item2) {
    if (item1.getUserAgent()) {
        return item1.getUserAgent();
    }
    return item2.getUserAgent();
}


function isEntryQuery(action) {
    return ['first', 'wordlist'].indexOf(action) >= 0;
}

function createFakeItem() {
    return {
        getRemoteAddr: function () { return null; },
        getUserAgent: function () {return null; }
    };
}

function mergeItems(item1, item2) {
    var action = fetchAction(item1, item2),
        entryQuery = isEntryQuery(action),
        corpnameElms = fetchCorpname(item1, item2);
    return {
        datetime: item1.getISODate(), // + timezone
        userId: fetchUserId(item1, item2),
        entryQuery: entryQuery,
        ipAddress: fetchIp(item1, item2),
        action: fetchAction(item1, item2),
        corpname: corpnameElms[0],
        limited: corpnameElms[1],
        userAgent: fetchUserAgent(item1, item2),
        procTime: fetchProcTime(item1, item2)
    };
}


function resolveGroup(values) {
    var i, j,
        ans = [],
        used1 = [], used2 = [],
        mismatches,
        testFunctions = [
            mismatchInIp, mismatchInAction, mismatchInAgent, mismatchInCorpname];

    for (i = 0; i < values.length; i += 1) {
        for (j = 0; j < i; j += 1) {
            if (used1.indexOf(i) < 0 && used2.indexOf(j) < 0) {
                mismatches = testFunctions.map(function (curr) {
                        return curr(values[i], values[j]) ? curr.name : null;
                    }).filter(function (item) { return item; });
                if (mismatches.length === 0) {
                    ans.push(mergeItems(values[i], values[j]));
                    used1.push(i);
                    used2.push(j);

                } else {
                    // mismatch too much
                }
            }
        }
    }
    return ans;
}



dataChunks(2, function (idx) {
    return [orzo.fileChunkReader(APACHE_LOG_FILE, idx),
            orzo.fileChunkReader(APPLOG_FILE, idx)];
});


applyItems(function (chunks, map) {
    while (chunks[0].hasNext()) {
        map(['apache', chunks[0].next()]);
    }
    while (chunks[1].hasNext()) {
        map(['app', chunks[1].next()]);
    }
});


map(function (item) {
    var id = item[0],
        data = item[1],
        parsed;


    if (id === 'apache') {
        parsed = parseApacheLine(data);

    } else if (id === 'app') {
        parsed = applog.parseLine(data);
    }

    if (parsed) {
        if (itemPasses(parsed)) {
            emit(parsed.getISODate().substr(0, 19), parsed);

        } else {
            emit('excluded', 1);
        }

    } else {
        emit('excluded', 1);
    }
});


reduce(2, function (key, values) {
    if (key !== 'excluded') {
        if (values.length === 1) {
            emit('result', mergeItems(values[0], createFakeItem()));

        } else {
            resolveGroup(values).forEach(function (item) {
                emit('result', item);
            });
        }

    } else {
        emit('excluded', D(values).size());
    }

});


finish(function (results) {
    doWith(
        orzo.fileWriter(OUTPUT_FILE),
        function (fw) {
            results.get('result').forEach(function (item) {
                fw.writeln(orzo.toJson(item));
            });
        },
        function (err) {
            orzo.print('error: ' + err);
        }
    );
    orzo.printf('Done. Number of excluded records: %s\n', results.get('excluded')[0]);
});