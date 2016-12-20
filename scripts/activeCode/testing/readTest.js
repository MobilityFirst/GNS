function run(value, field, querier) {
  if (field === "someField") {
    value["someField"] = "updated value";
  }
  return value;
}