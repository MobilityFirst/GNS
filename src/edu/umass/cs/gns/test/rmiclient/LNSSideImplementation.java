package edu.umass.cs.gns.test.rmiclient;


import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.QueryResult;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implements client RMI interface. This is a wrapper class around LocalNameServer.getIntercessor() and uses it to send requests
 * to GNS.
 *
 * Multiple instance of this class cannot run on same machine, as they bind to rmiregistry using the same name.
 *
 * Created by abhigyan on 5/25/14.
 */
@Deprecated
public class LNSSideImplementation implements ClientRMIInterface {

  public static String LNS_NAME = "Hello";

  public static int LNS_PORT = 42131;
  
  private ClientRequestHandlerInterface handler;

  public LNSSideImplementation(ClientRequestHandlerInterface handler) 
  {
    this.handler = handler;
  }
     
  @Override
  public NSResponseCode sendAddRecord(String name, String key, ResultValue value) throws RemoteException {
    return handler.getIntercessor().sendAddRecord(name, key, value);
  }

  @Override
  public NSResponseCode sendRemoveRecord(String name) throws RemoteException {
    return handler.getIntercessor().sendRemoveRecord(name);
  }

  @Override
  public QueryResult sendQueryBypassingAuthentication(String name, String key) throws RemoteException {
    return handler.getIntercessor().sendQueryBypassingAuthentication(name, key);
  }

  @Override
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, String newValue, String oldValue, UpdateOperation operation) throws RemoteException {
    return handler.getIntercessor().sendUpdateRecordBypassingAuthentication(name, key, newValue, oldValue, operation);
  }

  public static void startServer(ClientRequestHandlerInterface handler) {
    GNS.getLogger().info("Staring server .... ");
    try {
      LNSSideImplementation obj = new LNSSideImplementation(handler);
      ClientRMIInterface stub = (ClientRMIInterface) UnicastRemoteObject.exportObject(obj, 0);

      // Bind the remote object's stub in the registry
      Registry registry = LocateRegistry.getRegistry(LNS_PORT);
      registry.rebind(LNS_NAME, stub);
      GNS.getLogger().info("Staring server .... complete ....");
    } catch (Exception e) {
      System.err.println("Server exception: " + e.toString());
      e.printStackTrace();
    }

  }
}
