/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.test;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.GroupAccess;
import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.ThreadUtils;
import javax.swing.JOptionPane;
import java.util.ArrayList;

/**
 *
 * @author westy
 */
public class RemoveTest {

  private static void retrieveTest() throws Exception {
    int hostID = 2;
    int reply = JOptionPane.showConfirmDialog(null, "This will reset the GNS Database at the local host. "
            + "Are you sure you want to run this test?", "Reset the DB?", JOptionPane.YES_NO_OPTION);
    if (reply == JOptionPane.NO_OPTION) {
      //fail("user declined to run test");
      System.exit(-1);
    }
    ConfigFileInfo.readHostInfo("ns1", hostID);
    HashFunction.initializeHashFunction();
    Intercessor client = Intercessor.getInstance();
    GNS.getLogger().info("USING HOST ID #" + hostID);
    client.setLocalServerID(hostID);
    GNS.getLogger().info("RESETING THE DATABASE");
    client.sendResetDB();
    ThreadUtils.sleep(2000);
    String name = "Sally";
    String guid = "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24";
    String memberGuid = "829C3804401B0727F70F73D4415E162400CBE57B";
    String publicKey = "dummy3";
    GNS.getLogger().info("RECORD CREATION:");
    AccountAccess fred = new AccountAccess();
    fred.addAccount(name, guid, publicKey, guid);
    client.sendUpdateRecordWithConfirmation(guid, GroupAccess.GROUP, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);

//    GNS.getLogger().info("GSN LOOKUP:");
//    QueryResultValue result = client.sendQuery(name, AccountAccess.GUID);
//    GNS.getLogger().info(name + ": " + AccountAccess.GUID + " -> " + result.get(0));
//    QueryResultValue accountResult = client.sendQuery(guid, AccountAccess.ACCOUNT_INFO);
//    GNS.getLogger().info(guid + ": " + AccountAccess.ACCOUNT_INFO + " -> " + new AccountInfo(accountResult).toJSONObject().toString());
//    QueryResultValue guidResult = client.sendQuery(guid, AccountAccess.GUID_INFO);
//    GNS.getLogger().info(guid + ": " + AccountAccess.GUID_INFO + " -> " + new GuidInfo(guidResult).toJSONObject().toString());

    ArrayList<String> result = client.sendQuery(guid, GroupAccess.GROUP);
    GNS.getLogger().info(guid + ": " + GroupAccess.GROUP + " -> " + result);

    client.sendUpdateRecordWithConfirmation(guid, GroupAccess.GROUP, memberGuid, null, UpdateOperation.REMOVE);
    
    result = client.sendQuery(guid, GroupAccess.GROUP);
    GNS.getLogger().info(guid + ": " + GroupAccess.GROUP + " -> " + result);

  }

  public static void main(String[] args) throws Exception {
    retrieveTest();
    System.exit(0);
  }
}
