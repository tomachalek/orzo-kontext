/*
 * Filter out bot visits, remove duplicities in KonText app log
 */

/// <ref path="./orzojs.d.ts" />

var parser = require('applog');

dataChunks(2, function (idx) {
    //var d1 = 'd:/work/data/kontext-logs/test-merge/apache';
    var d1 = 'd:/work/data/kontext-logs/app2015';
    return orzo.directoryReader(d1, idx);
});


applyItems(function (dataChunk, map) {
    var fr,
        line,
        parsed,
        parsed2,
        lastErrLine,
        userAgent;


    while (dataChunk.hasNext()) {
        fr = orzo.fileReader(dataChunk.next());
        while (fr.hasNext()) {
            line = fr.next().trim();
            if (!line) {
                continue;
            }
            parsed = parser.parseLine(line);
            if (parsed && parsed.isOK()) {
                userAgent = parsed.getUserAgent();
                if (userAgent && (parser.agentIsMonitor(userAgent)
                        || parser.agentIsMonitor(userAgent))) {
                    continue;

                } else if (parsed.getType() === 'INFO') {
                    map(line);

                } else if (parsed.getType() === 'ERROR'
                        || parsed.getType() === 'WARNING') {
                    while (fr.hasNext()) {
                        line = fr.next().trim();
                        if (!line) {
                            continue;
                        }
                        parsed2 = parser.parseLine(line);
                        if (parsed2) {
                            parsed.data.error = lastErrLine;
                            map(line);
                            break;

                        } else {
                            lastErrLine = line;
                        }
                    }
                }
            }
        }
    }
});


map(function (line) {
    emit(orzo.hash.md5(line), line);
});


reduce(2, function (key, values) {
    emit('output', values[0]); // remove duplicities
    if (values.length > 1) {
        emit('duplicities', 1);
    }
});


finish(function (results) {
    var outputDir = 'd:/work/data/kontext-logs/cleaned';
    orzo.printf('Num of duplicities: %s\n', results.get('duplicities').length);
    doWith(orzo.fileWriter(outputDir + '/app-clean.log'), function (fw) {
        results.get('output').forEach(function (item) {
            fw.writeln(item);
        });
    });
});