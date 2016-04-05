function run(value, field, querier) {
	if (field == "name"){
		try{
			var q = querier.readGuid(null, "_count_name");
			var count = q.get("_count_name");
			querier.writeGuid(null, "_count_name", ++count);
		}catch(err){
			// If the counter field doesn't exist, initialize it to be 0
			querier.writeGuid(null, "_count_name", 0);
		}
	}
	return value;
}