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
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
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
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ReadTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;

  /**
   *
   */
  public ReadTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
      try {
        masterGuid = GUIDUtilsHTTPClient.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_01_CreateEntity() {
    try {
      GuidEntry testGuid = clientCommands.guidCreate(masterGuid, "testGUID" + RandomString.randomString(12));
      try {
        clientCommands.guidRemove(masterGuid, testGuid.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing guids: " + e);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_CreateField() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
    try {
      // remove default read acces for this test
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);

      // read my own field
      Assert.assertEquals("work",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another field
      Assert.assertEquals("000-00-0000",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));
      // read another field
      Assert.assertEquals("666flapJack",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "password", westyEntry));

      try {
        String result = clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry);
        Utils.failWithStackTrace("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception during read of westy's environment by sam: " + e);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_ACLPartOne() {
    try {
      try {
        clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment",
                samEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception adding Sam to Westy's readlist: " + e);
      }
      try {
        Assert.assertEquals("work",
                clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while Sam reading Westy's field: " + e);
      }
    } catch (Exception e) {
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
      clientCommands.guidRemove(masterGuid, samEntry.getGuid());

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }
}
