function run(value, accessor, querier) {
  if (value.hasOwnProperty("someField")) {
    value["someField"] = "updated value";
  }
  return value;
}