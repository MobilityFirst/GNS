function run(value, accessor, querier) {
  if (value.hasOwnProperty("test2")) {
    var aMap = {"anotherField":"anotherValue"};
    querier.writeGuid(null, "bogus", aMap);
  }
  return value;
}