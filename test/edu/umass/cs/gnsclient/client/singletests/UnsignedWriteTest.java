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
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test unsigned writes.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedWriteTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  /**
   *
   */
  public UnsignedWriteTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_01_CreateField() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      // remove default access
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), unsignedReadFieldName, "funkadelicread", westyEntry);
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, unsignedReadFieldName, GNSProtocol.ALL_GUIDS.toString());
      Assert.assertEquals("funkadelicread", clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedReadFieldName, null));

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), standardReadFieldName, "bummer", westyEntry);

      try {
        String result = clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), standardReadFieldName, null);
        Utils.failWithStackTrace("Result of read of westy's " + standardReadFieldName + " as world readable was " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_UnsignedWrite() {
    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      clientCommands.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, unsignedWriteFieldName, GNSProtocol.ALL_GUIDS.toString());
      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      Assert.assertEquals("funkadelicwrite", clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), standardWriteFieldName, "bummer", westyEntry);
      try {
        clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), standardWriteFieldName, "funkadelicwrite", null);
        Utils.failWithStackTrace("Write of westy's field " + standardWriteFieldName + " as world readable should have been rejected.");
      } catch (ClientException e) {
        // normal result
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_04_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

}
