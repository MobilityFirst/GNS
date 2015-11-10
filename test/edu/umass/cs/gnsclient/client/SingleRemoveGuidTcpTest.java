/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.client.util.Utils;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.io.IOException;
import java.net.InetSocketAddress;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SingleRemoveGuidTcpTest {

  private static String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;

  public SingleRemoveGuidTcpTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  @Order(1)
  public void test_01_RemoveGuidUsingAccount() {
    String testGuidName = "testGUID" + Utils.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: " + e);
    }
    try {
      client.guidRemove(masterGuid, testGuid.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
    try {
    client.lookupGuidRecord(testGuid.getGuid());
     fail("Lookup testGuid should have throw an exception.");
    } catch (GnsException e) {
      
    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }
  
}
