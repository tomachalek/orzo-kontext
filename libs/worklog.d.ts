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

declare module "worklog" {

    export interface Worklog {

        /**
         * @param path Path to a log file
         * @param startTime A time when an operation started
         * @param defaultCheckInterval How old (in seconds) record should be created
         * in case the log is empty
         */
        constructor(path:string, startTime:number|Date, defaultCheckInterval:number);

        /**
         * Write startTime to the log
         */
        close():void;

        /**
         * Returns the latest item from the log
         */
        getLatestTimestamp():number;
    }
}