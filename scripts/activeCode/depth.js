function run(value, field, querier){
    var map = querier.readGuid(null,"depthField");
    var depth = map.get("depthField");
   
    return value.put(field,depth);
}