function run(value, accessor, querier) {
  if (value.hasOwnProperty("test1")) {
    value["test1"] = "updated value1";
  }
  return value;
}