package edu.umass.cs.gns.test.testclient;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implements client RMI interface. This is a wrapper class around Intercessor and uses it to send requests
 * to GNS.
 *
 * Multiple instance of this class cannot run on same machine, as they bind to rmiregistry using the same name.
 *
 * Created by abhigyan on 5/25/14.
 */
public class LNSSideImplementation implements ClientRMIInterface {

  public static String LNS_NAME = "Hello";

  public LNSSideImplementation() {}
  @Override
  public NSResponseCode sendAddRecord(String name, String key, ResultValue value) throws RemoteException {
    return Intercessor.sendAddRecord(name, key, value);
  }

  @Override
  public NSResponseCode sendRemoveRecord(String name) throws RemoteException {
    return Intercessor.sendRemoveRecord(name);
  }

  @Override
  public QueryResult sendQueryBypassingAuthentication(String name, String key) throws RemoteException {
    return Intercessor.sendQueryBypassingAuthentication(name, key);
  }

  @Override
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, String newValue, String oldValue, UpdateOperation operation) throws RemoteException {
    return Intercessor.sendUpdateRecordBypassingAuthentication(name, key, newValue, oldValue, operation);
  }

  public static void startServer() {
    GNS.getLogger().info("Staring server .... ");
    try {
      LNSSideImplementation obj = new LNSSideImplementation();
      ClientRMIInterface stub = (ClientRMIInterface) UnicastRemoteObject.exportObject(obj, 0);

      // Bind the remote object's stub in the registry
      Registry registry = LocateRegistry.getRegistry();
      registry.rebind(LNS_NAME, stub);
      GNS.getLogger().info("Staring server .... complete ....");
    } catch (Exception e) {
      System.err.println("Server exception: " + e.toString());
      e.printStackTrace();
    }

  }
}
