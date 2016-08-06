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
package edu.umass.cs.gnsclient.client.singletests;


import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;

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

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  public UnsignedWriteTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void test_01_CreateField() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_02_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      // remove default access
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSCommandProtocol.ALL_FIELDS, GNSCommandProtocol.ALL_GUIDS);

      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedReadFieldName, "funkadelicread", westyEntry);
      client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, unsignedReadFieldName, GNSCommandProtocol.ALL_GUIDS);
      assertEquals("funkadelicread", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedReadFieldName, null));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardReadFieldName, "bummer", westyEntry);

      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), standardReadFieldName, null);
        fail("Result of read of westy's " + standardReadFieldName + " as world readable was " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_03_UnsignedWrite() {
    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      client.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, unsignedWriteFieldName, GNSCommandProtocol.ALL_GUIDS);
      client.fieldReplaceFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      assertEquals("funkadelicwrite", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardWriteFieldName, "bummer", westyEntry);
      try {
        client.fieldReplaceFirstElementTest(westyEntry.getGuid(), standardWriteFieldName, "funkadelicwrite", null);
        fail("Write of westy's field " + standardWriteFieldName + " as world readable should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

}
