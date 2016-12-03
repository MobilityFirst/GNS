function run(value, field, querier) {
	//substitute this line with the targetGuid
	querier.writeGuid(targetGuid, "someField", value);
	return value;
}