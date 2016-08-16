/**
 * Created by gaozy on 1/24/16.
 */

function run(value, field, querier){
    if(field == "nextGuid") {
        var nextGuid = value.get("nextGuid");
        if(nextGuid != "") {
            nextGuid = querier.readGuid(nextGuid, "nextGuid");
        }
    }
    return value;
}
