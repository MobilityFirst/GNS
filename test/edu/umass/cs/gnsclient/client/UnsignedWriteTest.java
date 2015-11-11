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
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.GnsProtocol.AccessType;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.net.InetSocketAddress;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedWriteTest {

  private static String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client = null;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  public UnsignedWriteTest() {
    if (client == null) {
      address = ServerSelectDialog.selectServer();
      System.out.println("Connecting to " + address.getHostName() + ":" + address.getPort());
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
  public void test_01_CreateField() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  @Order(2)
  public void test_02_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      // remove default access
      client.aclRemove(AccessType.READ_WHITELIST, westyEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
      
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedReadFieldName, "funkadelicread", westyEntry);
      client.aclAdd(AccessType.READ_WHITELIST, westyEntry, unsignedReadFieldName, GnsProtocol.ALL_USERS);
      assertEquals("funkadelicread", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedReadFieldName, null));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardReadFieldName, "bummer", westyEntry);
      
      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), standardReadFieldName, null);
        fail("Result of read of westy's " + standardReadFieldName + " as world readable was " + result
                + " which is wrong because it should have been rejected.");
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  @Order(3)
  public void test_03_UnsignedWrite() {
    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      client.aclAdd(AccessType.WRITE_WHITELIST, westyEntry, unsignedWriteFieldName, GnsProtocol.ALL_USERS);
      client.fieldReplaceFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      assertEquals("funkadelicwrite", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardWriteFieldName, "bummer", westyEntry);
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(), standardWriteFieldName, "funkadelicwrite", null);
        fail("Write of westy's field " + standardWriteFieldName + " as world readable should have been rejected.");
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

}
