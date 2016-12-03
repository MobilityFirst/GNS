function run(value, field, querier){
	var targetGuid = value.get("someField");
	if(targetGuid !="Depth query succeeds!"){
		value.put(field, querier.readGuid(targetGuid, "someField").get("someField"));
	}
    return value;
}