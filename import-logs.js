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

(function () {
    'use strict';

    var numMapWorkers = 2;
    var numReduceWorkers = 1;
    var rec2elastic = require('rec2elastic');
    var BULK_URL = orzo.sprintf('http://localhost:9200/logs/_bulk');
    //var SOURCE_FILE = 'd:/work/data/kontext-logs/test-100k/output.txt';
    var SOURCE_FILE = 'd:/work/data/kontext-logs/test-250k/output.txt';
    var CHUNK_SIZE = 5000;

    dataChunks(numMapWorkers, function (idx) {
        return orzo.fileChunkReader(SOURCE_FILE, idx);
    });

    applyItems(function (dataChunk, map) {
        while (dataChunk.hasNext()) {
            map(JSON.parse(dataChunk.next()));
        }
    });

    map(function (item) {
        var meta = rec2elastic.createMetaRecord(item, 'kontext');
        emit(item.datetime.substr(0, 10), [orzo.toJson(meta), orzo.toJson(item)]);
	});

    reduce(numReduceWorkers, function (key,  values) {
        var bulkInsert = new rec2elastic.BulkInsert(BULK_URL, CHUNK_SIZE, false);
        bulkInsert.insertValues(values);
    });

    finish(function (results) {
        orzo.print('DONE');
    });

}());
