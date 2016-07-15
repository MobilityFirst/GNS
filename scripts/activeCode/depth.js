function run(value, field, querier){
    var depth = querier.readGuid(null,"activeField");
    depth = depth -1;
    while(depth>0){
    	querier.readGuid(null,"activeField");
    	depth--;
    }
    return value;
}