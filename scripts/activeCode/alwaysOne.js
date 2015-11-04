/**
 * @author: Westy
 */
function run(value, field, querier) {
    if (value.opt("name") != null) {
	  value.put("name", 1);
	}
	return value;
}
