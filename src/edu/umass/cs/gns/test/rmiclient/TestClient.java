package edu.umass.cs.gns.test.rmiclient;


import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Client which communicates with local name server and tests add, remove, lookup, and update for a single name.
 *
 * It uses Java RMI for communicating with local name server and can be used also if the local name server is on a
 * remote machine.
 *
 * Usage: java -ea -cp $gns_jar  -Djava.rmi.server.useCodebaseOnly=false -Djava.rmi.server.codebase=file:$gns_jar
 *                     edu.umass.cs.gns.test.rmiclient.TestClient  LNS_HOSTNAME(optional argument)
 *
 * Created by abhigyan on 5/25/14.
 */
public class TestClient{
  private TestClient() {}

  public static void main(String[] args) {
    String host = (args.length < 1) ? null : args[0];
    try {
      Registry registry = LocateRegistry.getRegistry(host);
      ClientRMIInterface stub = (ClientRMIInterface) registry.lookup(LNSSideImplementation.LNS_NAME);

      String name = "test_name_" + Util.randomString(10);

      String key = "test_key";

      // add a name
      String initialValue = "ABCD";

      NSResponseCode response = stub.sendAddRecord(name, key, getResultValue(initialValue));
      assert response == NSResponseCode.NO_ERROR: "Error in adding record";

      // do a lookup and check if same value is returned
      QueryResult result = stub.sendQueryBypassingAuthentication(name, key);

      assert (result.get(key).get(0)).equals(initialValue);

      // do an update
      String value = "PQRS";
      response = stub.sendUpdateRecordBypassingAuthentication(name, key,
              value, null, UpdateOperation.REPLACE_ALL);
      assert response == NSResponseCode.NO_ERROR: "Error in updating record";

      // check if same value is returned
      result = stub.sendQueryBypassingAuthentication(name, key);
      assert (result.get(key).get(0)).equals(value);

      // remove the record
      response = stub.sendRemoveRecord(name);
      assert response == NSResponseCode.NO_ERROR;

      GNS.getStatLogger().info("Client for 1 name successful. Client exiting.");
      System.exit(0);
    } catch (Exception e) {
      System.err.println("Client exception: " + e.toString());
      e.printStackTrace();
    }
  }

  private static ResultValue getResultValue(String s){
    ResultValue value = new ResultValue();
    value.add(s);
    return value;
  }
}
