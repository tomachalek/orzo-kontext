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

(function () {
    'use strict';

    var applogParser = require('applog');
    var numMapWorkers = 2;
    var numReduceWorkers = 1;
    var BULK_URL = orzo.sprintf('http://localhost:9200/kontext/_bulk');
    var CHUNK_SIZE = 1000;

    dataChunks(numMapWorkers, function (idx) {
        return orzo.directoryReader(env.inputArgs[0], idx);
    });

    applyItems(function (dataChunk, map) {
        var fr,
            line,
            parsed,
            parsed2,
            lastErrLine;


        while (dataChunk.hasNext()) {
            fr = orzo.fileReader(dataChunk.next());
            while (fr.hasNext()) {
                line = fr.next().trim();
                if (!line) {
                    continue;
                }
                parsed = applogParser.parseLine(line);
                if (parsed) {
                    if (parsed.isOK()) {
                        if (parsed.getType() === 'INFO') {
                            map(parsed);

                        } else if (parsed.getType() === 'ERROR'
                                || parsed.getType() === 'WARNING') {
                            while (fr.hasNext()) {
                                line = fr.next().trim();
                                if (!line) {
                                    continue;
                                }
                                parsed2 = applogParser.parseLine(line);
                                if (parsed2) {
                                    parsed.data.error = lastErrLine;
                                    map(parsed2);
                                    break;

                                } else {
                                    lastErrLine = line;
                                }
                            }
                        }
                    }
                }
            }
        }
    });


    map(function (item) {
        var corpname;
        var data = {};

        try {
            corpname = item.data.params && item.data.params.corpname ? decodeURIComponent(item.data.params.corpname) : null;

        } catch (e) {
            data.error = e;
            corpname = item.data.params.corpname;
        }

        data.datetime = item.getISODate();
        data.userId = item.data.user_id;
        data.procTime = item.data.proc_time;
        data.action = item.data.action;
        data.corpname = corpname;

        var meta = {
            index : {
                _id : orzo.hash.sha1(JSON.stringify(data)),
                _type : 'applog'
            }
        };
        emit(data.datetime[data.datetime.length - 1],
            [JSON.stringify(meta), JSON.stringify(data)]);
	});

    function insertRecords(data) {
        // orzo.print(buff);
        orzo.rest.post(BULK_URL, data.trim());
    }

    reduce(numReduceWorkers, function (key,  values) {
        var buff = '';
        values.forEach(function (item, i) {
            if (i > 0 && i % CHUNK_SIZE === 0) {
                insertRecords(buff);
                buff = '';
            }
            buff += '\n' + item.join('\n');
        });
        if (buff) {
            insertRecords(buff);
        }
    });

    finish(function (results) {
        orzo.print('DONE');
    });

}());
