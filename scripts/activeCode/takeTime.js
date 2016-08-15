var thread =  Java.type("java.lang.Thread");

function run(value, field, querier){
    thread.sleep(1000*value.get(field));
    return value;
}