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

declare module "apachelog" {

    export interface DateParser {
        (str:string):Date;
    }

    export interface LineParser {
        (str:string):{[key:string]:any};
    }

    export var dateParsers: {
        parseDMYDatetime:DateParser,
        parseYMDDatetime:DateParser
    };

    export var lineParsers: {
        parseLine:LineParser,
        parseSkeLine:LineParser
    }

    /**
     */
    export function createParser(lineParser:LineParser, dateParser:DateParser,
            pathPrefix:string);
}