function run(value, field, querier) {
	//substitute this line with the targetGuid
	var newVal = querier.readGuid(targetGuid, "depthField");
	value["someField"] = newVal["depthField"];
	return value;
}