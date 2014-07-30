package edu.umass.cs.gns.test.basictest;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;

/**
 * Simple test for testing add, remove, lookup, and update for a single name.
 *
 * Created by abhigyan on 5/23/14.
 */
public class Test1Name extends Thread {

  public static void startTest() {
      new Thread(new Test1Name()).start();
  }

  public void run() {
    String name = "test_name_" + Util.randomString(10);

    String key = "test_key";

    // add a name
    String initialValue = "ABCD";

    NSResponseCode response = Intercessor.sendAddRecord(name, key, getResultValue(initialValue));
    assert response == NSResponseCode.NO_ERROR: "Error in adding record";

    // do a lookup and check if same value is returned
    QueryResult result = Intercessor.sendQueryBypassingAuthentication(name, key);

    assert (result.getArray(key).get(0)).equals(initialValue);

    // do an update
    String value = "PQRS";
    response = Intercessor.sendUpdateRecordBypassingAuthentication(name, key,
            value, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL);
    assert response == NSResponseCode.NO_ERROR: "Error in updating record";

    // check if same value is returned
    result = Intercessor.sendQueryBypassingAuthentication(name, key);
    assert (result.getArray(key).get(0)).equals(value);

    // remove the record
    response = Intercessor.sendRemoveRecord(name);
    assert response == NSResponseCode.NO_ERROR;

    GNS.getStatLogger().info("Basic test for 1 name successful. Local name server exiting.");
    System.exit(0);
  }

  private ResultValue getResultValue(String s){
    ResultValue value = new ResultValue();
    value.add(s);
    return value;
  }

}
