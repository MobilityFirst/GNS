/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.GnsProtocol;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.client.util.Utils;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidBatchTcpClientTest {

  private static String ACCOUNT_ALIAS = "david@westy.org"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;

  public CreateGuidBatchTcpClientTest() {

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
  public void test_01_CreateBatch() {
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      aliases.add("testGUID" + Utils.randomString(6));
    }
    String result = null;
    int oldTimeout = client.getReadTimeout();
    try {
       client.setReadTimeout(10 * 60 * 1000); // 10 minutes
       result = client.guidBatchCreate(masterGuid, aliases);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GnsProtocol.OK_RESPONSE, result);
    client.setReadTimeout(oldTimeout);
  }

}
