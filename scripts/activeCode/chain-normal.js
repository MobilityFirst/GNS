/**
 * Created by gaozy on 1/24/16.
 * This code is for test use only.
 */

function run(value, field, querier){
    if(field == "testGuid") {
        var nextGuid = value.get("testGuid");
        if(nextGuid != "") {
            querier.readGuid(nextGuid, "guid2");
        }
    }
    return value;
}
