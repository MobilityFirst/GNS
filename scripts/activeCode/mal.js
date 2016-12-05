/**
 * Created by gaozy on 1/28/16.
 */

function run(value, field, querier) {
	/**
	 * The maximal value is 2^32-1 to create an array
	 * creating an array does not mean the memory of
	 * the array is allocated. 
	 */
	var size=4294967295;
	var arr = new Array(size);
	for(var i=0;i<size;i++){
		arr[i] = i;
	}
    return value;
}
