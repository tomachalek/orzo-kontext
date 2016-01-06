/// <ref path="./libs/orzojs.d.ts" />

/*
2001:718:1e03:ffc1::4
-
-
[05/Oct/2014:11:35:12 +0200]
"GET /run.cgi/first?shuffle=1&reload=&corpname=omezeni%2Fsyn2010&queryselector=iqueryrow&iquery=%C4%8De%C5%99it&lemma=&lpos=&phrase=&word=&wpos=&char=&cql=&default_attr=word&fc_lemword_window_type=both&fc_lemword_wsize=5&fc_lemword=&fc_lemword_type=all&fc_pos_window_type=both&fc_pos_wsize=5&fc_pos_type=all HTTP/1.0" 500 6463 "-" "Python-urllib/1.17" t=11449
*/
var apachelog = require('apachelog');
var applog = require('applog');
var elastic = require('rec2elastic');
var logsDir = 'd:/work/data/kontext-logs/apache2014';
var numWorkers = 3;

dataChunks(numWorkers, function (idx) {
    return orzo.directoryReader(logsDir, idx);
});


processChunk(function (chunk, map) {
    while (chunk.hasNext()) {
        map(chunk.next());
    }
});

function parseLine(s) {
    //apachelog.parseDMYDatetime
    return apachelog.lineParsers.parseLine(s);
}


function isEntryQuery(q) {
    if (q) {
        if (q.indexOf('/wordlist') > -1) {
            return true;

        } else if (q.indexOf('/first') > -1 && q.indexOf('/first_form') === -1) {
            return true;

        } else {
            return false;
        }
    }
    return false;
}

function isHuman(agent) {
    return !applog.agentIsBot(agent) && !applog.agentIsMonitor(agent);
}

function isAction(q) {
    return q.indexOf('.png') === -1 && q.indexOf('.js') === -1
        && q.indexOf('.jpg') === -1 && q.indexOf('.css') === -1
        && q.indexOf('.gif') === -1 && q.indexOf('.ico') === -1;
}

map(function (item) {
    doWith(
        orzo.fileReader(item),
        function (fr) {
            var line;
            var parsed;
            var datetime;
            var meta;
            var doc;

            while (fr.hasNext()) {
                line = fr.next();
                parsed = parseLine(line);
                if (parsed && isHuman(parsed.userAgent)) {
                    if (isAction(parsed.query)) {
                        datetime = apachelog.dateParsers.parseDMYDatetime(parsed.time);
                        if (datetime && datetime.getFullYear() === 2014) {
                            meta = elastic.createMetaRecord(parsed, 'kontext');
                            doc = {
                                datetime: applog.dateToISOPrev(datetime),
                                entryQuery: isEntryQuery(parsed.query) ? true : false,
                                query: parsed.query
                            };
                            emit('value', [orzo.toJson(meta), JSON.stringify(doc)]);
                        } else {
                            //orzo.print('ignored date: ' + datetime);
                        }
                    }
                }
            }
        },
        function (err) {
            orzo.print(err);
        }
    );
});


reduce(numWorkers, function (key, values) {
    var url = 'http://localhost:9200/kontext-2014/_bulk';
    var bulkInsert = new elastic.BulkInsert(url, 5000, false);
    bulkInsert.setPrintInserts(false);
    //orzo.dump(values[0]);
    bulkInsert.insertValues(values);
});

finish(function (results) {
    results.each(function (key, values) {

    });
});