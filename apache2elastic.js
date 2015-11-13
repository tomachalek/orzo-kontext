/*
 * Copyright (C) 2015 Tomas Machalek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// <reference path="./orzojs.d.ts" />
/// <reference path="./libs/applog.d.ts" />
/// <reference path="./libs/apachelog.d.ts" />

(function () {
    'use strict';

    var apacheParser = require('apachelog');
    var applog = require('applog');
    var rec2elastic = require('rec2elastic');
    var numMapWorkers = 2;
    var numReduceWorkers = 1;
    var month = 10;
    var BULK_URL = orzo.sprintf('http://localhost:9200/kontext/_bulk');
    var CHUNK_SIZE = 20000;

    var parseLine = apacheParser.createParser(apacheParser.lineParsers.parseLine,
            apacheParser.dateParsers.parseDMYDatetime);


    dataChunks(numMapWorkers, function (idx) {
        return orzo.directoryReader(env.inputArgs[0], idx);
    });

    applyItems(function (dataChunk, map) {
        var fr;
        var line;
        var parsed;

        while (dataChunk.hasNext()) {
            fr = orzo.fileReader(dataChunk.next());
            while (fr.hasNext()) {
                line = fr.next().trim();
                parsed = parseLine(line);
                if (parsed) {
                    if (parsed.getDate().getMonth() === month - 1 && parsed.action != 'files') {
                        map(parsed);
                    }

                } else {
                    orzo.print('unreadable: ' + line);
                }
            }
        }
    });

    map(function (record) {
        var rec = rec2elastic.convertRecord(record);

        if (applog.agentIsBot(record.getUserAgent())) {
            emit('bot', 1);

        } else if (applog.agentIsMonitor(record.getUserAgent())) {
            emit('monitor', 1);

        } else {
            emit(rec.datetime, [rec.metadata, rec.data]);
            emit('default', 1);
        }
    });

    reduce(numReduceWorkers, function (key,  values) {
        if (['bot', 'monitor', 'default'].indexOf(key) > - 1) {
            emit(key, D(values).size());

        } else {
            new rec2elastic.BulkInsert(BULK_URL, CHUNK_SIZE, true).insertValues(values);
        }
    });

    finish(function (results) {
        results.each(function (key, values) {
           orzo.printf('%s --> %s\n', key, values[0]);
        });
        orzo.print('done');
    });

}());

