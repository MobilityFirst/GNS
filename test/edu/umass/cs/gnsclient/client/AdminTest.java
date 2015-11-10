/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import java.net.InetSocketAddress;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AdminTest {

  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;

  public AdminTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
    }
  }

  @Test
  public void test_01_AdminEnter() {
    try {
      client.adminEnable(address.getHostName()+":8080");
    } catch (Exception e) {
      fail("Exception while enabling admin mode: " + e);
    }
  }
  
  @Test
  public void test_02_ParameterGet() {
    try {
      String result = client.parameterGet("email_verification");
      assertEquals("true", result);
    } catch (Exception e) {
      fail("Exception while enabling admin mode: " + e);
    }
  }
  
  @Test
  public void test_03_ParameterSet() {
    try {
      client.parameterSet("max_guids", 2000);
    } catch (Exception e) {
      fail("Exception while enabling admin mode: " + e);
    }
    try {
      String result = client.parameterGet("max_guids");
      assertEquals("2000", result);
    } catch (Exception e) {
      fail("Exception while enabling admin mode: " + e);
    }
  }

}
