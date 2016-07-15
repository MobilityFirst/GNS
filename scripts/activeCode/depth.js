function run(value, field, querier){
    var map = querier.readGuid(null,"activeField");
    var depth = map.getInt("activeField") - 1;
    while(depth>0){
    	querier.readGuid(null,"activeField");
    	depth--;
    }
    return value;
}