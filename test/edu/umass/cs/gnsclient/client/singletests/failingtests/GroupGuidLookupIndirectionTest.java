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
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.Arrays;

import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;

import org.json.JSONException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * THIS TEST FAILS BECAUSE SUPPORT FOR INDIRECT FIELD READING WITH GROUPS
 * WAS LOST A FEW HUNDRED COMMITS AGO
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupGuidLookupIndirectionTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  private static final String indirectionGroupTestFieldName = "_IndirectionTestQueryField_";
  private static GuidEntry indirectionGroupGuid;
  private static JSONArray indirectionGroupMembers = new JSONArray();
  
  private static final Set<GuidEntry> createdGuids = new HashSet<>();

  /**
   *
   */
  public GroupGuidLookupIndirectionTest() {
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
        Utils.failWithStackTrace("Exception while creating account guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_10_SetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "guid-" + RandomString.randomString(12));
        createdGuids.add(testEntry);
        indirectionGroupMembers.put(testEntry.getGuid());
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry, indirectionGroupTestFieldName, array);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_20_RegisterGroup() {
    try {
      indirectionGroupGuid = clientCommands.guidCreate(masterGuid, "indirectionGroup-" + RandomString.randomString(12));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_30_AddGroupMembers() {
    try {
      clientCommands.groupAddGuids(indirectionGroupGuid.getGuid(), 
              indirectionGroupMembers, indirectionGroupGuid);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_40_TestRead() {
    try {
      String actual = clientCommands.fieldRead(indirectionGroupGuid, indirectionGroupTestFieldName);
      System.out.println("Indirection Test Result = " + actual);
      String expected = new JSONArray(Arrays.asList(Arrays.asList(25), Arrays.asList(25), Arrays.asList(25), Arrays.asList(25), Arrays.asList(25))).toString();
      JSONAssert.assertEquals(expected, actual, true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while trying to read the " + indirectionGroupTestFieldName + " of the group guid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_50_Cleanup() {
    try {
      for (GuidEntry guid : createdGuids) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      createdGuids.clear();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

}
