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
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import java.net.InetSocketAddress;
import java.util.Arrays;
import org.json.JSONArray;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupGuidLookupIndirectionTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;
  
  private static final String indirectionGroupTestFieldName = "_IndirectionTestQueryField_";
  private static GuidEntry indirectionGroupGuid;
  private JSONArray IndirectionGroupMembers = new JSONArray();

  public GroupGuidLookupIndirectionTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void test_01_SetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
        IndirectionGroupMembers.put(testEntry.getGuid());
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry, indirectionGroupTestFieldName, array);
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      indirectionGroupGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTestGroup-" + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      client.groupAddGuids(indirectionGroupGuid.getGuid(), IndirectionGroupMembers, masterGuid);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  public void test_02_TestRead() {
    try {
      String actual = client.fieldRead(indirectionGroupGuid, indirectionGroupTestFieldName);
      System.out.println("Indirection Test Result = " + actual);
      String expected = new JSONArray(Arrays.asList(Arrays.asList(25), Arrays.asList(25), Arrays.asList(25), Arrays.asList(25), Arrays.asList(25))).toString();
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to read the " + indirectionGroupTestFieldName + " of the group guid: " + e);
    }
  }

}
