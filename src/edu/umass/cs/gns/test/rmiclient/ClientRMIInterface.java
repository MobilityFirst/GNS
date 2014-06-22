package edu.umass.cs.gns.test.rmiclient;

import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Local name server exposes these methods to client. The signature of these methods is copied
 * from {@link edu.umass.cs.gns.clientsupport.Intercessor}
 *
 * It includes four type of queries for respectively adding, removing, reading, and writing to a name
 * in GNS.
 *
 * Created by abhigyan on 5/25/14.
 */
public interface ClientRMIInterface extends Remote{

  public NSResponseCode sendAddRecord(String name, String key, ResultValue value)throws RemoteException;
  public NSResponseCode sendRemoveRecord(String name)throws RemoteException;
  public QueryResult sendQueryBypassingAuthentication(String name, String key)throws RemoteException;
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, String newValue,
                                                         String oldValue, UpdateOperation operation)throws RemoteException;

}
