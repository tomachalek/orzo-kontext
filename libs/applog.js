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

    /**
     * Creates a Date object using passed integer representing values from
     * year down to milliseconds. The 'month' value requires values from 1 to 12
     * (i.e. unlike JS Data constructor).
     */
    function newDate(year, month, day, hour, minute, second, millisecond) {
        return new Date(year, month - 1, day, hour, minute, second, millisecond);
    }

    /**
     *
     * @param ds
     * @returns {*}
     */
    function parseDatetimeString(ds) {
        var items = ds.split(/[T\s]/),
            date,
            time,
            millis,
            ans;

        if (items.length === 2) {
            date = items[0].split('-');
            time = items[1].split(':');
            time[2] = time[2].split(',');
            millis = time[2][1];
            time[2] = time[2][0];
            if (millis === undefined) {
                millis = '0';
            }
            time[3] = millis;
            items = date.concat(time).map(function (item) { return parseInt(item, 10); });
            return newDate.apply(null, items);

        } else if (items.length === 1) {
            date = items[0].split('-').map(function (item) { return parseInt(item, 10); });
            return newDate.apply(null, date);

        } else {
            ans = undefined;
        }
        return ans;
    }

    /**
     *
     * @param datetime
     * @param source
     * @param type
     * @param rawData
     * @returns {Record}
     */
    function createRecord(datetime, source, type, rawData) {
        var data;

        if (type === 'INFO') {
            try {
                data = JSON.parse(rawData);

            } catch (e) {
                data = {corrupted: true, error: e};
            }

        } else {
            data = {error: null};
        }

        function Record(data) {
            this.data = data;
        }

        Record.prototype._metadata = {
            date : datetime,
            source : source,
            type : type,
            id : orzo.hash.sha1(rawData)
        };

        Record.prototype.getId = function () {
            return this._metadata.id;
        };

        Record.prototype.getTimestamp = function () {
            return this._metadata.date.getTime() / 1000;
        };

        Record.prototype.getDate = function () {
            return this._metadata.date;
        };

        Record.prototype.getISODate = function () {
            var d = this.getDate();
            return orzo.sprintf('%02d-%02d-%02dT%02d:%02d:%02d,%s',
                    d.getFullYear(), d.getMonth() + 1,  d.getDate(), d.getHours(),
                    d.getMinutes(), d.getSeconds(), d.getMilliseconds());
        }

        Record.prototype.getSource = function () {
            return this._metadata.source;
        };

        Record.prototype.getType = function () {
            return this._metadata.type;
        };

        Record.prototype.isOK = function () {
            return !this.data.hasOwnProperty('corrupted');
        };

        Record.prototype.contains = function (s) {
            return Boolean(RegExp(s).exec(this.getSource()));
        };

        Record.prototype.isOlderThan = function (dt) {
            var timestamp;

            if (typeof dt === 'string') {
                timestamp = parseDatetimeString(dt).getTime() / 1000;

            } else {
                timestamp = dt.getTime() / 1000;
            }
            return this.timestamp() < timestamp;
        };

        return new Record(data);
    }

    /**
     * Parses KonText applog record
     */
    lib.parseLine = function (line) {
        // 2015-06-01 13:30:36,925 [QUERY] INFO: {... etc }
        var srch = /(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+\[([^\]]+)\]\s([A-Z]+):(.+)/
                .exec(line);

        if (srch) {
            return createRecord(
                parseDatetimeString(srch[1]),
                srch[2],
                srch[3],
                srch[4].trim()
            );

        } else {
            return null;
        }
    };

    module.exports = lib;

}(module));