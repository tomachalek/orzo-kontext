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

    function importDatetime(d) {
        if (Object.prototype.toString.call(d) === '[object Date]') {
            return d.getTime() / 1000;

        } else if (typeof d === 'string') {
            return parseFloat(d);

        } else if (typeof d === 'number') {
            return d;

        } else {
            throw new Error('Incompatible datetime type');
        }
    }

    function Worklog(path, startTime, defaultCheckInterval) {
        this.path = path;
        this.startTime = importDatetime(startTime);
        this.defaultCheckInterval = defaultCheckInterval;
        if (orzo.fs.exists(this.path)) {
            this.data = orzo.readTextFile(this.path).trim().split(/\s+/);

        } else {
            this.data = [this.startTime - this.defaultCheckInterval];
        }
    }

    Worklog.prototype.close = function () {
        var self = this;

        this.data.push(this.startTime);
        doWith(
            orzo.fileWriter(this.path),
            function (fw) {
                self.data.forEach(function (item) {
                    fw.writeln(item);
                });
            },
            function (err) {
                orzo.print('error: ' + err);
            }
        );
    };

    Worklog.prototype.getLatestTimestamp = function () {
        return this.data[this.data.length - 1];
    };

    module.exports.Worklog = Worklog;


    module.exports.validateConf = function (c) {
        var props = ['srcDir', 'archivePath', 'bulkUrl', 'chunkSize', 'defaultCheckInterval',
                     'workLogPath'];
        props.forEach(function (item) {
            if (c[item] === undefined) {
                throw new Error(orzo.sprintf('Missing configuration item "%s"', item));
            }
        });
        return c;
    };


    module.exports.fileIsInRange = function (filePath, fromDate, parserFn) {
        var reader = orzo.reversedFileReader(filePath);
        var lastLine = null;
        var parsed = null;
        var ans = false;
        while (reader.hasNext()) {
            lastLine = reader.next();
            if (lastLine) {
                parsed = parserFn(lastLine);
                if (parsed && parsed.isOK()) {
                    if (parsed.getDate().getTime() / 1000 < fromDate) {
                        break;

                    } else {
                        ans = true;
                        break;
                    }
                }
            }
        }
        reader.close();
        return ans;
    };




}(module));
