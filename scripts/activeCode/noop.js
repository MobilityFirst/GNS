/**
 * @author: Zhaoyu Gao
 */

var thread = Java.type("java.lang.Thread");

function run(value, field, querier) {
	thread.sleep(5);
	return value;
}
