/**
 * Created by gaozy on 1/24/16.
 */

var thread =  Java.type("java.lang.Thread");

function run(value, field, querier){
    if(field == "nextGuid") {
        var nextGuid = value.get("nextGuid");
        if(nextGuid != "") {
            querier.readGuid(nextGuid, "nextGuid");
        }
        thread.sleep(500);
        while(true)
            ;
    }
    return value;
}
