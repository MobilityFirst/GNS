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

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
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
public class SingleReadTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;

  public SingleReadTest() {
    if (client == null) {
      InetSocketAddress address = ServerSelectDialog.selectServer();
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
  public void test_01_CreateEntity() {
    try {
      GuidUtils.registerGuidWithTestTag(client, masterGuid, "testGUID" + RandomString.randomString(6));
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  @Order(3)
  public void test_02_CreateSubGuid() {
    try {
      subGuidEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "subGuid" + RandomString.randomString(6));
      System.out.println("Created: " + subGuidEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  @Order(3)
  public void test_03_CreateField() {
    try {
      client.fieldCreateOneElementList(subGuidEntry.getGuid(), "environment", "work", subGuidEntry);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during create field: " + e);
    }
  }

  @Test
  @Order(4)
  public void test_04_ReadFieldTwice() {
    try {
      // read my own field
      assertEquals("work", client.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry));
      assertEquals("work", client.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry));
      ThreadUtils.sleep(5);
      assertEquals("work", client.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry));
      ThreadUtils.sleep(5);
      assertEquals("work", client.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (Exception e) {
      fail("Exception reading field: " + e);
      e.printStackTrace();
    }
  }
}
