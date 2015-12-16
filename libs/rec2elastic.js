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
     * Creates a meta-data record for bulk insert
     */
    lib.createMetaRecord = function (item, type) {
        return {
            index : {
                _id : orzo.hash.sha1(orzo.toJson(item)),
                _type : type
            }
        };
    };

    function isEntryQuery(action) {
        return ['first', 'wordlist'].indexOf(action) >= 0;
    }

    function importCorpname(item) {
        var corpname,
            limited;

        if (item.data.params && item.data.params.corpname) {
            corpname = item.data.params.corpname;
            corpname = decodeURIComponent(corpname);
            corpname = corpname.split(';')[0];
            if (corpname.indexOf('omezeni/') === 0) {
                corpname = corpname.substr('omezeni/'.length);
                limited = true;

            } else {
                limited = false;
            }
            return [corpname, limited];

        } else {
            return [null, null];
        }
    }

    function importGeoData(ipAddress, data) {
        return {
            continent_code: null, // TODO
            country_code2: data.countryISO,
            country_code3: null,
            country_name: data.countryName,
            ip: ipAddress,
            latitude: data.latitude,
            location: [data.latitude, data.longitude],
            longitude: data.longitude,
            timezone: null // TODO
        };
    }

    /**
     * Converts an applog record to CNK's internal format
     * designed for storing an application request information.
     */
    lib.convertRecord = function (item, type, geoInfo) {
        var corpnameElms;
        var data = {};
        geoInfo = geoInfo || {};

        corpnameElms = importCorpname(item);

        data.datetime = item.getISODate();
        data.type = type;
        data.userId = item.data.user_id;
        data.procTime = item.data.proc_time;
        data.action = item.data.action;
        data.entryQuery = isEntryQuery(data.action);
        data.corpus = corpnameElms[0];
        data.limited = corpnameElms[1];
        data.userAgent = item.getUserAgent();
        data.ipAddress = item.getRemoteAddr();
        data.geoip = importGeoData(data.ipAddress, geoInfo);

        var meta = lib.createMetaRecord(data, type);

        return {
            datetime: data.datetime[data.datetime.length - 1],
            metadata: JSON.stringify(meta),
            data: JSON.stringify(data)
        };
    };

    /**
     * Bulk insert helper object
     */
    function BulkInsert(url, itemsPerChunk, dryRun) {
        this._url = url;
        this._itemsPerChunk = itemsPerChunk;
        this._dry_run = dryRun || false;
        this._print_inserts = false;
    }

    BulkInsert.prototype._insert = function (data) {
        if (!this._dry_run) {
            orzo.rest.post(this._url, data.trim());
        }
        if (this._print_inserts) {
            orzo.printf('---> %s\n', data);
        }
    };

    BulkInsert.prototype.setPrintInserts = function (v) {
        this._print_inserts = v;
    };

    /**
     * Inserts list of values
     */
    BulkInsert.prototype.insertValues = function (values) {
        var buff = '';
        var self = this;

        values.forEach(function (item, i) {
            if (i > 0 && i % self._itemsPerChunk === 0) {
                self._insert(buff);
                buff = '';
            }
            buff += '\n' + item.join('\n');
        });
        if (buff) {
            this._insert(buff);
        }
        if (self._dry_run) {
            orzo.printf('dummy insert of %d items\n', values.length);
        }
    };

    lib.BulkInsert = BulkInsert;

    module.exports = lib;

}(module));
