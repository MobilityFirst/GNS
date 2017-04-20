function run(value, accessor, querier) {
  if (value.hasOwnProperty("test2")) {
    var readValue = querier.readGuid("someField", null);
    value["test1"] = readValue["someField"];
  }
  return value;
}
