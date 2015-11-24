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
     */
    function containsAll(str) {
        var srch = Array.prototype.slice.call(arguments, 1);
        return srch.reduce(function (prev, curr) {
            return prev && str.indexOf(curr) > -1;
        }, true);
    }

    /**
     *
     */
    lib.agentIsBot = function (agentStr) {
        /*
        Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
        Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)
        Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)
        Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)
        Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)
        Mozilla/5.0 (compatible; SeznamBot/3.2; +http://fulltext.sblog.cz/)
        Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)
        /*/
        agentStr = agentStr ? agentStr.toLowerCase() : '';
        return containsAll(agentStr, 'googlebot')
            || containsAll(agentStr, 'ahrefsbot')
            || containsAll(agentStr, 'yandexbot')
            || containsAll(agentStr, 'yahoo', 'slurp')
            || containsAll(agentStr, 'baiduspider')
            || containsAll(agentStr, 'seznambot')
            || containsAll(agentStr, 'bingbot');
    }

    /**
     *
     */
    lib.agentIsMonitor = function (agentStr) {
        /*
        Python-urllib/2.7
        Zabbix-test
        */
        agentStr = agentStr ? agentStr.toLowerCase() : '';
        return containsAll(agentStr, 'python-urllib/2.7')
            || containsAll(agentStr, 'zabbix-test');
    }

    /**
     *
     */
    lib.dateToISO = function (d) {
        return orzo.sprintf('%02d-%02d-%02dT%02d:%02d:%02d,%s',
            d.getFullYear(), d.getMonth() + 1,  d.getDate(), d.getHours(),
            d.getMinutes(), d.getSeconds(), d.getMilliseconds());
    };

    /**
     *
     */
    lib.createRecord = function (datetime, source, type, jsonData) {
        var data;

        if (type === 'INFO') {
            try {
                if (typeof jsonData === 'string') {
                    data = JSON.parse(jsonData);

                } else if (typeof jsonData === 'object') {
                    data = jsonData;

                } else {
                    throw new Error('Invalid rawData object type: ' + (typeof jsonData));
                }

            } catch (e) {
                data = {corrupted: true, error: e};
            }

        } else {
            data = {error: null};
        }

        function Record(data) {
            this.data = data;
        }

        Record.prototype.toString = function () {
            var desc = {
                'date': this.getISODate(),
                'type': this.getType(),
                'data': this.data
            };
            return orzo.toJson(desc);
        };

        Record.prototype._metadata = {
            date : datetime,
            source : source,
            type : type,
            id : orzo.hash.sha1(jsonData)
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
            return lib.dateToISO(this.getDate());
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

        Record.prototype.getUserAgent = function () {
            if (this.data && this.data.request) {
                return this.data.request['HTTP_USER_AGENT'];
            }
            return null;
        }

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
    };

    /**
     * Parses KonText applog record
     */
    lib.parseLine = function (line) {
        /*
        parses KonText applog string like this one:
        2015-06-01 13:30:36,925 [QUERY] INFO: {... JSON object ... }
        */
        var srch = /(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+\[([^\]]+)\]\s([A-Z]+):(.+)/
                .exec(line);

        if (srch) {
            return lib.createRecord(
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