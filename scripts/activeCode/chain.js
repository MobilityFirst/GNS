/**
 * Created by gaozy on 1/24/16.
 */

var thread =  Java.type("java.lang.Thread");

function run(value, field, querier){
    if(field == "testGuid") {
        var nextGuid = value.get("testGuid");
        if(nextGuid != "") {
            querier.readGuid(nextGuid, "testGuid");
        }
        thread.sleep(500);
        while(true)
            ;
    }
    return value;
}