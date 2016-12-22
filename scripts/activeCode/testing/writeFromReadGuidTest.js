function run(value, accessor, querier) {
  if (value.hasOwnProperty("test2")) {
    var map = querier.readGuid(null, "test1");
    map["test1"] = "newTest1Value";
    querier.writeGuid(null, "bogus", map);
  }
  return value;
}