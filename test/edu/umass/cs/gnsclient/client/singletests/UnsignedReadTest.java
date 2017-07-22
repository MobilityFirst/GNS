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
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test unsigned reads.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedReadTest extends DefaultGNSTest {

  private static final String PASSWORD = "password";
  private static GNSClientCommands clientCommands = null;

  /**
   *
   */
  public UnsignedReadTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: ", e);
      }
    }
  }

  private static GuidEntry unsignedReadAccountGuid;

  /**
   *
   */
  @Test
  public void test_249_UnsignedReadDefaultWriteCreateAccountGuid() {
    try {
      unsignedReadAccountGuid = clientCommands.accountGuidCreate(
              "unsignedReadAccountGuid" + RandomString.randomString(12), PASSWORD);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception writing field UnsignedReadDefaultMasterWrite: ", e);
    }
  }

  /**
   * Tests for the default ACL list working with unsigned read.
   * Sets up the field in the account guid.
   */
  @Test
  public void test_250_UnsignedReadDefaultAccountGuidWrite() {
    try {
      clientCommands.fieldUpdate(unsignedReadAccountGuid, "aRandomFieldForUnsignedRead", "aRandomValue");
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception writing field UnsignedReadDefaultMasterWrite: ", e);
    }
  }

  /**
   * Tests for the default ACL list working with unsigned read.
   * Attempts to read the field.
   */
  @Test
  public void test_251_UnsignedReadDefaultAccountGuidRead() {
    try {
      String response = clientCommands.fieldRead(unsignedReadAccountGuid.getGuid(), "aRandomFieldForUnsignedRead", null);
      Assert.assertEquals("aRandomValue", response);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception writing field UnsignedReadDefaultMasterWrite: ", e);
    }
  }

  private static GuidEntry unsignedReadTestGuid;
  private static final String unsignedReadFieldName = "allreadaccess";
  private static final String unreadAbleReadFieldName = "cannotreadreadaccess";

  /**
   * Create the subguid for the unsigned read tests.
   */
  @Test
  public void test_252_UnsignedReadCreateGuids() {
    try {
      unsignedReadTestGuid = clientCommands.guidCreate(unsignedReadAccountGuid, "unsignedReadTestGuid" + RandomString.randomString(12));
      System.out.println("Created: " + unsignedReadTestGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception registering guids in UnsignedReadCreateGuids: ", e);
    }
  }

  /**
   * Check the default ACL for the unsigned read tests.
   * The account guid and EVERYONE should be in the ENTIRE_RECORD ACL.
   */
  @Test
  public void test_253_UnsignedReadCheckACL() {
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(unsignedReadAccountGuid.getGuid(),
              GNSProtocol.EVERYONE.toString())));
      JSONArray actual = clientCommands.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, false);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while retrieving ACL in UnsignedReadCheckACL: ", e);
    }
  }

  /**
   * Write the value the unsigned read tests.
   */
  @Test
  public void test_254_UnsignedReadDefaultWrite() {
    try {
      clientCommands.fieldUpdate(unsignedReadTestGuid.getGuid(),
              unsignedReadFieldName, "funkadelicread", unsignedReadTestGuid);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while writing value UnsignedReadDefaultWrite: ", e);
    }
  }

  /**
   * Check the value the unsigned read tests.
   */
  @Test
  public void test_255_UnsignedReadDefaultRead() {
    try {
      Assert.assertEquals("funkadelicread",
              clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), unsignedReadFieldName, null));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading value in UnsignedReadDefaultRead: ", e);
    }
  }

  /**
   * Remove the default ENTIRE_RECORD read access.
   */
  @Test
  public void test_256_UnsignedReadFailRemoveDefaultReadAccess() {
    try {
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception removing defa in UnsignedReadDefaultRead: ", e);
    }
  }

  /**
   * Ensure that only the master guid is in the ACL.
   */
  @Test
  public void test_257_UnsignedReadCheckACLForRecord() {
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(unsignedReadAccountGuid.getGuid())));
      JSONArray actual = clientCommands.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, false);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while retrieving ACL in UnsignedReadCheckACL: ", e);
    }
  }

  /**
   * Write a value to the field we're going to try to read.
   */
  @Test
  public void test_258_UnsignedReadFailWriteField() {

    try {
      clientCommands.fieldUpdate(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, "bummer", unsignedReadTestGuid);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   * Attempt a read that should fail because ENTIRE_RECORD was removed.
   */
  @Test
  public void test_259_UnsignedReadFailRead() {
    try {
      try {
        String result = clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, null);
        Utils.failWithStackTrace("Result of read of westy's "
                + unreadAbleReadFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have been rejected in UnsignedRead.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   * Adds access for just the field we are trying to read.
   */
  @Test
  public void test_260_UnsignedReadAddFieldAccess() {
    try {
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, unsignedReadTestGuid, unsignedReadFieldName,
              GNSProtocol.ALL_GUIDS.toString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception adding unsigned access in UnsignedReadAddFieldAccess: ", e);
    }
  }

  /**
   * Insures that we can read a world readable field without a guid.
   * This one has an ALL_GUIDS ACL just for this field.
   */
  @Test
  public void test_261_UnsignedReadWithFieldAccess() {
    try {
      Assert.assertEquals("funkadelicread", clientCommands.fieldRead(unsignedReadTestGuid.getGuid(),
              unsignedReadFieldName, null));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while testing for unsigned access in UnsignedReadAddReadWithFieldAccess: ", e);
    }
  }

  /**
   * Insures that we still can't read the non-world-readable field without a guid.
   */
  @Test
  public void test_262_UnsignedReadFailAgain() {
    try {
      try {
        String result = clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, null);
        Utils.failWithStackTrace("Result of read of westy's "
                + unreadAbleReadFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have been rejected in UnsignedRead.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_263_UnsignedReadFailMissingField() {
    String missingFieldName = "missingField" + RandomString.randomString(12);
    try {
      try {
        String result = clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), missingFieldName, null);
        Utils.failWithStackTrace("Result of read of test guid's nonexistant field "
                + missingFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have failed.");
      } catch (ClientException e) {
        // The normal result
      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_264_Cleanup() {
    try {
      clientCommands.guidRemove(unsignedReadAccountGuid, unsignedReadTestGuid.getGuid());
      clientCommands.accountGuidRemove(unsignedReadAccountGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }
}
