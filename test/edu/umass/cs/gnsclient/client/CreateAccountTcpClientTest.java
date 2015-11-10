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
import java.net.InetSocketAddress;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateAccountTcpClientTest {

  private static String ACCOUNT_ALIAS = "david@westy.org"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;

  public CreateAccountTcpClientTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
    }
  }

  @Test
  public void test_01_CreateAccount() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_02_CheckAccount() {
    String guidString = null;
    try {
      guidString = client.lookupGuid(ACCOUNT_ALIAS);
    } catch (Exception e) {
      fail("Exception while looking up guid: " + e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = client.lookupAccountRecord(guidString);
      } catch (Exception e) {
        fail("Exception while looking up account record: " + e);
      }
    }
    if (json == null) {
      try {
        assertFalse(json.getBoolean(GnsProtocol.ACCOUNT_RECORD_VERIFIED));
      } catch (Exception e) {
        fail("Exception while getting field from account record: " + e);
      }
    }
  }

}
