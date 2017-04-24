function run(value, field, querier) {
	//substitute this line with the targetGuid
	var newVal = querier.readGuid("depthField", targetGuid);
	value["someField"] = newVal["depthField"];
	return value;
}
