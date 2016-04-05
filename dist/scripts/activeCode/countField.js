/**
 * @author: Westy
 */
 function run(value, field, querier) {
	if (value.opt("name") != null) {
		try{
			var q = querier.readGuid(null, "_count_name");
			var count = q.get("_count_name");
			querier.writeGuid(null, "_count_name", ++count);
		}catch(err){
			// If the counter field doesn't exist, initialize it to be 1
			querier.writeGuid(null, "_count_name", 1);
		}
	}
	return value;
}