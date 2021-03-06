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

declare module "applog" {

    export interface Record {

        /**
         *
         */
        getId():string;

        /**
         * Returns UNIX epoch time in seconds
         */
        getTimestamp():number;

        /**
         *
         */
        getDate():Date;

        /**
         * Returns a ISO datetime string. Format details are
         * specified by setDateFormat().
         */
        getISODate():string;

        /**
         * Returns original line string. It is always available
         * (even if isOK returns false).
         */
        getSource():string;

        /**
         *
         */
        getType():string;

        /**
         * Tests whether the record is valid (= it was
         * parsed successfully as a known type).
         */
        isOK():boolean;

        /**
         * Tests whether the provided pattern can
         * be found in record's source. Pattern is
         * interpreted as a regular expression.
         *
         * @param pattern
         */
        contains(pattern:string):boolean;

        /**
         *
         * @param date
         */
        isOlderThan(date:Date):boolean;

        /**
         * Set date format version:
         * 0 - original version (2015-12-14T18:37:51,931)
         * 1 - updated version with tz and US decimal separator
         *     (2015-12-14T18:37:51.931+01:00)
         *
         * Default version is '0'.
         */
        setDateFormat(f:number):void;

        /**
         * @return one of {basic, lemma, phrase, word, char, cql}
         */
        getQueryType():string;
    }

    /**
     * Parses KonText application log line.
     *
     * @param line
     */
    export function parseLine(line:string):Record;

    /**
     * Creates a universal application log Record
     *
     * @param datetime log date
     * @param source original log record string
     * @param type - one of INFO, WARNING, ERROR, FATAL
     * @param jsonData - JSON data
     */
    export function createRecord(datetime:Date, source:string, type:string, jsonData:string|{[key:string]:any});

    /**
     * Converts a Date instance into an ISO form
     * [year]-[month]-[day]T[hours]:[minutes][seconds].[milliseconds][timezone offset]
     */
    export function dateToISO(datetime:Date):string;

    /**
     * Converts a number of seconds to a triple (hours, minutes, seconds).
     */
    export function timeToHMS(t:number):Array<number>;

    /**
     * Tests whether the provided user-agent string matches any of
     * known web spiders
     */
    export function agentIsBot(agentStr:string):boolean;

    /**
     * Tests whether the provided user-agent string matches any of
     * known internal monitoring services
     */
    export function agentIsMonitor(agentStr:string):boolean;

}