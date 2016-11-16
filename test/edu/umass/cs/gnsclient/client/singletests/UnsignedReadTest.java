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
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedReadTest {

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public UnsignedReadTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        failWithStackTrace("Exception creating client: ", e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        failWithStackTrace("Exception when we were not expecting it: ", e);
      }
    }
  }

  private static GuidEntry unsignedReadTestGuid;

  // Creating 4 fields
  private static final String unsignedReadFieldName = "allreadaccess";
  private static final String standardReadFieldName = "standardreadaccess";
  private static final String unsignedOneReadFieldName = "allonereadaccess";
  private static final String standardOneReadFieldName = "standardonereadaccess";

  /**
   *
   */
  @Test
  public void test_250_UnsignedReadCreateGuids() {
    try {
      unsignedReadTestGuid = client.guidCreate(masterGuid, "unsignedReadTestGuid" + RandomString.randomString(6));
      System.out.println("Created: " + unsignedReadTestGuid);
    } catch (Exception e) {
      failWithStackTrace("Exception registering guids in UnsignedReadCreateGuids: ", e);
    }
    try {
      // For this test we remove default all access for reading
      client.aclRemove(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing ACL in UnsignedReadCreateGuids: ", e);
    }
    // Only the account guid should be in the ACL - check this here
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(masterGuid.getGuid())));
      JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      failWithStackTrace("Exception while retrieving ACL in UnsignedReadCreateGuids: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_251_UnsignedRead() {

    // Insures that we can read a world readable field without a guid
    try {
      client.fieldUpdate(unsignedReadTestGuid.getGuid(), unsignedReadFieldName, "funkadelicread", unsignedReadTestGuid);
      // Add all access to this field
      client.aclAdd(AclAccessType.READ_WHITELIST, unsignedReadTestGuid, unsignedReadFieldName, GNSProtocol.ALL_GUIDS.toString());
      assertEquals("funkadelicread", client.fieldRead(unsignedReadTestGuid.getGuid(), unsignedReadFieldName, null));
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for unsigned access in UnsignedRead: ", e);
    }
    // Insures that we can't read non-world-readable field without a guid
    try {
      client.fieldUpdate(unsignedReadTestGuid.getGuid(), standardReadFieldName, "bummer", unsignedReadTestGuid);
      try {
        String result = client.fieldRead(unsignedReadTestGuid.getGuid(), standardReadFieldName, null);
        failWithStackTrace("Result of read of westy's "
                + standardReadFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have been rejected in UnsignedRead.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_252_UnsignedReadOne() {
    try {
      // Insures that we can read a world readable field without a guid
      client.fieldCreateOneElementList(unsignedReadTestGuid.getGuid(),
              unsignedOneReadFieldName, "funkadelicread", unsignedReadTestGuid);
      client.aclAdd(AclAccessType.READ_WHITELIST, unsignedReadTestGuid, unsignedOneReadFieldName, GNSProtocol.ALL_GUIDS.toString());
      assertEquals("funkadelicread", client.fieldReadArrayFirstElement(
              unsignedReadTestGuid.getGuid(), unsignedOneReadFieldName, null));

    } catch (Exception e) {
      failWithStackTrace("Exception while testing for unsigned access in UnsignedReadOne: ", e);
    }
    try {
      // Insures that we can't read non-world-readable field without a guid
      try {
        client.fieldCreateOneElementList(unsignedReadTestGuid.getGuid(),
                standardOneReadFieldName, "bummer", unsignedReadTestGuid);
        String result = client.fieldReadArrayFirstElement(
                unsignedReadTestGuid.getGuid(), standardOneReadFieldName, null);
        failWithStackTrace("Result of read of "
                + standardOneReadFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was '"
                + result
                + "' which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in UnsignedReadOne: ", e);
    }
  }

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }
}
