/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.test;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.AccountInfo;
import edu.umass.cs.gns.client.Admintercessor;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.ThreadUtils;

import javax.swing.*;
import java.util.ArrayList;
import java.util.Arrays;

//import edu.umass.cs.gnrs.nameserver.MongoRecordMap;
//import edu.umass.cs.gnrs.nameserver.NameRecord;
//import edu.umass.cs.gnrs.nameserver.RecordMapInterface;
//import edu.umass.cs.gns.packet.QueryResultValue;

/**
 *
 * @author westy
 */
public class RetrievalTest {

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
    Admintercessor admin = Admintercessor.getInstance();
    Intercessor client = Intercessor.getInstance();
    GNS.getLogger().info("USING HOST ID #" + hostID);
    client.setLocalServerID(hostID);
    admin.setLocalServerID(hostID);
    GNS.getLogger().info("RESETING THE DATABASE");
    admin.sendResetDB();
    ThreadUtils.sleep(2000);
    String name = "Sally";
    String guid = "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24";
    String publicKey = "dummy3";
    GNS.getLogger().info("RECORD CREATION:");
    client.sendAddRecordWithConfirmation(name, AccountAccess.GUID, new ResultValue(Arrays.asList(guid)));
    AccountInfo accountInfo = new AccountInfo(name, guid, null);
    client.sendAddRecordWithConfirmation(guid, AccountAccess.ACCOUNT_INFO, accountInfo.toDBFormat());
    GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
    client.sendUpdateRecordWithConfirmation(guid, AccountAccess.GUID_INFO, guidInfo.toDBFormat(), null, UpdateOperation.CREATE);
    //ThreadUtils.sleep(3000);
//    GNS.getLogger().info("DIRECT RETRIEVAL FROM DATABASE:");
//    RecordMapInterface recordMap = new MongoRecordMap();
//    NameRecord nameRecord = recordMap.getNameRecord(guid);
//    if (nameRecord == null) {
//      GNS.getLogger().info("Record was not found!!");
//    } else {
//      GNS.getLogger().info("" + nameRecord);
//      GNS.getLogger().info("" + nameRecord.get(AccountAccess.ACCOUNT_INFO));
//      GNS.getLogger().info("" + nameRecord.get(AccountAccess.GUID_INFO));
//    }
    GNS.getLogger().info("GSN LOOKUP:");
    ResultValue result = client.sendQuery(name, AccountAccess.GUID);
    GNS.getLogger().info(name + ": " + AccountAccess.GUID + " -> " + result.get(0));
    ResultValue accountResult = client.sendQuery(guid, AccountAccess.ACCOUNT_INFO);
    GNS.getLogger().info(guid + ": " + AccountAccess.ACCOUNT_INFO + " -> " + new AccountInfo(accountResult).toJSONObject().toString());
    ResultValue guidResult = client.sendQuery(guid, AccountAccess.GUID_INFO);
    GNS.getLogger().info(guid + ": " + AccountAccess.GUID_INFO + " -> " + new GuidInfo(guidResult.toResultValueString()).toJSONObject().toString());

  }

  public static void main(String[] args) throws Exception {
    retrieveTest();
    System.exit(0);
  }
}
