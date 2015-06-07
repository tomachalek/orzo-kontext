/// <reference path="./orzojs.d.ts" />
/// <reference path="./libs/applog.d.ts" />

(function () {
    'use strict';

    var applogParser = require('applog');
    var numMapWorkers = 4;
    var numReduceWorkers = 2;

    dataChunks(numMapWorkers, function (idx) {
        return orzo.directoryReader(env.inputArgs[0], idx);
    });

    applyItems(function (dataChunk, map) {
        var fr,
            line,
            parsed;

        while (dataChunk.hasNext()) {
            fr = orzo.fileReader(dataChunk.next());
            while (fr.hasNext()) {
                line = fr.next();
                parsed = applogParser.parseLine(line);
                if (parsed) {
                    if (parsed.isOK()) {
                        if (parsed.getType() === 'INFO' && parsed.data.proc_time) {
                            map([parsed.getDate(), parsed.data.proc_time]);

                        } else if (parsed.getType() === 'ERROR'
                                || parsed.getType() === 'WARNING') {
                            map([parsed.getDate(), parsed.getType().toLowerCase()]);
                        }
                    }
                }
            }
        }
    });

    function mapByMonths(data) {
        if (data[1] !== 'error' && data[1] !== 'warning') {
            emit(orzo.sprintf('%s-%s', data[0].getFullYear(), data[0].getMonth() + 1), data[1]);

        } else {
            emit(orzo.sprintf('%s-%s-%s', data[1], data[0].getFullYear(), data[0].getMonth() + 1), 1);
        }
    }


    map(mapByMonths);

    reduce(numReduceWorkers, function (key,  values) {
        emit(key, D(values).size());
        emit(key + '-procTime', D(values).average());
        emit(key + '-procTime-stdev', D(values).stdev());
    });

    finish(function (results) {
        results.sorted.each(function (key, values) {
            orzo.printf('%s --> %s\n', key, values[0].toFixed(1));
        });
    });

}());
