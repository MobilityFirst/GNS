var thread =  Java.type("java.lang.Thread");

function run(value, field, querier){
    thread.sleep(value.get(field));
    return value;
}