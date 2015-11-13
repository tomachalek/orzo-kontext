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

(function (module) {
    'use strict';

    var lib = {};

    lib.convertRecord = function (item) {
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

        return {
            datetime: data.datetime[data.datetime.length - 1],
            metadata: JSON.stringify(meta),
            data: JSON.stringify(data)
        };
    };

    function BulkInsert(url, itemsPerChunk, dryRun) {
        this._url = url;
        this._itemsPerChunk = itemsPerChunk;
        this._dry_run = dryRun || false;
    }

    BulkInsert.prototype._insert = function (data) {
        if (!this._dry_run) {
            orzo.rest.post(this._url, data.trim());

        } else {
            orzo.printf('>> %s\n', data);
        }
    };

    BulkInsert.prototype.insertValues = function (values) {
        var buff = '';
        var self = this;

        values.forEach(function (item, i) {
            orzo.print('>>>>>>>> ' + item);
            if (i > 0 && i % self._itemsPerChunk === 0) {
                orzo.print('fuck_______');
                self._insert(buff);
                buff = '';
                orzo.print('fuck_______222');
            }
            buff += '\n' + item.join('\n');
        });
        if (buff) {
            this._insert(buff);
        }
    };

    lib.BulkInsert = BulkInsert;

    module.exports = lib;

}(module));
