/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.client;

import java.io.IOException;
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

  private static GnsClient client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;

  public AdminTest() {
    if (client == null) {
      if (System.getProperty("host") != null
              && !System.getProperty("host").isEmpty()
              && System.getProperty("port") != null
              && !System.getProperty("port").isEmpty()) {
        address = new InetSocketAddress(System.getProperty("host"),
                Integer.parseInt(System.getProperty("port")));
      } else {
        address = new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
      }
     try {
        client = new GnsClient(address, System.getProperty("disableSSL").equals("true"));
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
    }
  }

  @Test
  public void test_01_AdminEnter() {
    try {
      client.adminEnable(address.getHostName() + ":8080");
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
