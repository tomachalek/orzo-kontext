declare module "applog" {
	
	export interface Record {

		/**
		 * Returns UNIX epoch time in seconds
		 */
		getTimestamp():number;

		/**
		 * Tests whether the record is valid (= it was
		 * parsed successfully as a known type).
		 */
		isOK():boolean;

		/**
		 *
		 */
		getDate():Date;

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
	}

	/**
	 * Parses KonText application log line.
	 *
	 * @param line
	 */
	export function parseLine(line:string):Record;

}