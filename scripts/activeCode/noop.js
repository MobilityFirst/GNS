/**
 * @author: Zhaoyu Gao
 */

var thread =  Java.type("java.lang.Thread");
function run(value, field, querier) {
	var t = Date.now();
    thread.sleep(4);
    while(Date.now() - t < 5)
        ;
	return value;
}
