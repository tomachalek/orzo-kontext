/*
 * Merge Apache and Application logs
 */

/// <ref path="./orzojs.d.ts" />

var apacheParser = require('apachelog');
var applog = require('applog');

var parseApacheLine = apacheParser.createParser(apacheParser.lineParsers.parseLine,
        apacheParser.dateParsers.parseDMYDatetime);


dataChunks(1, function (idx) {
    var d1 = 'd:/work/data/kontext-logs/merge-test/apache',
        d2 = 'd:/work/data/kontext-logs/merge-test/app';

    return [orzo.directoryReader(d1, idx), orzo.directoryReader(d2, idx)];
});


applyItems(function (chunks, map) {
    while (chunks[0].hasNext()) {
        doWith (orzo.fileReader(chunks[0].next()), function (fr) {
            while (fr.hasNext()) {
                map(['apache', fr.next()]);
            }
        });
    }
    while (chunks[1].hasNext()) {
        doWith (orzo.fileReader(chunks[1].next()), function (fr) {
            while (fr.hasNext()) {
                map(['app', fr.next()]);
            }
        });
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
    orzo.dump(parsed);
});


reduce(6, function (key, values) {

});


finish(function (results) {

});