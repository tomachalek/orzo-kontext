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


    var apache = {},
        dateParsers = {},
        lineParsers = {},
        applog = require('applog');

    /**
     *
     */
    function parseParams(q) {
        var items,
            params = {};

        q = q.split('?')[1];
        if (q) {
            items = q.split('&');
            items.forEach(function (item) {
                var tmp;

                if (item) {
                    tmp = item.split('=');
                    params[tmp[0]] = decodeURIComponent(tmp[1]);
                }
            });
        }
        return params;
    }

    /**
     *
     */
    function getCorpusAction(pathPrefix, query) {
        var srch = new RegExp('^' + pathPrefix + '/([\\w_]*)').exec(query);
        if (srch) {
            return srch[1] ? srch[1] : '/';
        }
        return null;
    }

    function filterCorpusParams(params) {
        var ans = {},
            p,
            allowed = [
                'iquery', 'lemma', 'phrase', 'word', 'char', 'cql',
                'queryselector', 'corpname', 'subcname', 'q'
            ];

        for (p in params) {
            if (params.hasOwnProperty(p) && allowed.indexOf(p) > -1) {
                ans[p] = params[p];
            }
        }
        return ans;
    }

    // ----------------------------------------------------------


    dateParsers.parseDMYDatetime = function (d) {
        // 15/Dec/2013:10:35:26 +0100
        var months = {
            'Jan': 0, 'Feb': 1, 'Mar': 2, 'Apr': 3,
            'May': 4, 'Jun': 5, 'Jul': 6, 'Aug': 7,
            'Sep': 8, 'Oct': 9, 'Nov': 10, 'Dec': 11
            },
            srch = /(\d{2})\/([A-Z][a-z]{2})\/(\d{4}):(\d{2}):(\d{2}):(\d{2})/.exec(d);

        if (srch) {
            var x = new Date(
                parseInt(srch[3], 10),
                parseInt(months[srch[2]], 10),
                parseInt(srch[1], 10),
                parseInt(srch[4], 10),
                parseInt(srch[5], 10),
                parseInt(srch[6], 10));
            if (isNaN(x.getFullYear())) {
                orzo.print('Failed to parse date: ' + d);
            }
            return x;
        }
        return null;
    };

    dateParsers.parseYMDDatetime = function (d) {
        var srch = /(\d{4})-(\d{2})-(\d{2})_(\d{2}):(\d{2}):(\d{2})/.exec(d);

        if (srch) {
            var x = new Date(
                parseInt(srch[1], 10),
                parseInt(srch[2], 10) - 1,
                parseInt(srch[3], 10),
                parseInt(srch[4], 10),
                parseInt(srch[5], 10),
                parseInt(srch[6], 10));
            if (isNaN(x.getFullYear())) {
                orzo.print('Failed to parse date: ' + d);
            }
            return x;
        }
        return null;
    };


    // 2001:718:1e03:ffc1::17 - - [15/Feb/2015:02:21:39 +0100]
    // "GET /first_form?corpname=omezeni/syn2010 HTTP/1.1" 200
    // 126627 "-" "Zabbix-test" t=707870

    lineParsers.parseLine = function (line) {
        var regexp = /([0-9a-f:\.]+)\s+-\s-\s\[([^\]]+)\]\s+"([^"]+)"\s+(\d+)\s+(\d+)\s+"([^"]+)"\s+"([^"]+)"\s+t=(\d+)(\s+pid=\d+)?/,
            ans,
            pref,
            q;

         line = line.replace(/\\"/g, '#');
         ans = regexp.exec(line);

        if (ans) {
            pref = /^([^\s]+)/.exec(ans[1]);
            q = ans[3].split(' ')[1];
            q = q.replace(/#/g, '\\"');
            return {
                ip: pref ? pref[1] : null,
                time: ans[2],
                userAgent: ans[7],
                procTime: parseInt(ans[8]) / 1e6,
                query: q
            }
        }
        return null;
    };

    /**
     * // 2013-12-29_06:58:26	89.24.228.111	novak    /ske/js/tblex.js ?foo=bar&another=stuff
     * @param line
     * @returns {{ip: *, time: *, query: *, user: *}}
     */
    lineParsers.parseSkeLine = function (line) {
        var items = line.split(/\s+/);
        return {
            ip : items[1],
            time : items[0],
            query : items[3] + items[4],
            user : items[2]
        };
    };

    apache.createParser = function (lineParser, dateParser, pathPrefix) {
        /*
        {
            "user_id": 1980,
            "proc_time": 1.0399,
            "pid": 38309,
            "request": {
                "HTTP_USER_AGENT": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36",
                "REMOTE_ADDR": "86.49.8.224"
             },
             "action": "first_form",
             "params": {
                 "corpname": "omezeni/syn2010"
             },
             "date": "2015-11-05 16:21:37"
         }
         */
        return function (line, callback) {
            var action,
                corpusMeta,
                parsedLine,
                applogItem,
                record;

            pathPrefix = pathPrefix || '';

            if (line) {
                parsedLine = lineParser(line);
                if (parsedLine) {
                    action = getCorpusAction(pathPrefix, parsedLine.query);
                    corpusMeta = filterCorpusParams(parseParams(parsedLine.query));

                    if (action) {
                        applogItem = {};
                        applogItem.user_id = null; // cannot fetch from Apache log
                        applogItem.proc_time = parsedLine.procTime;
                        applogItem.request = {
                            HTTP_USER_AGENT: parsedLine.userAgent,
                            REMOTE_ADDR: parsedLine.ip
                        };
                        applogItem.action = action;
                        applogItem.params = {
                            corpname: corpusMeta.corpname,
                            subcname: corpusMeta.subcname
                        }
                        record = applog.createRecord(
                            dateParser(parsedLine.time),
                            null,  // there is no source Python module available
                            'INFO',
                            applogItem
                        );

                        if (typeof callback === 'function') {
                            callback.call(parsedLine, parsedLine);
                        }
                        return record;

                    } else {
                        //orzo.printf('Non-action URL: %s\n', line);
                    }
                }
            }
            return null;
        };
    };

    apache.dateParsers = dateParsers;
    apache.lineParsers = lineParsers;
    module.exports = apache;

}(module));