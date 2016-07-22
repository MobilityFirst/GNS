function run(value, field, querier) {
	//substitute this line with the targetGuid
	value.put(field, querier.readGuid());
	return value;
}