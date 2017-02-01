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
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.Arrays;

import java.util.HashSet;
import java.util.Set;
import org.hamcrest.Matchers;
import org.json.JSONArray;

import org.json.JSONException;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SelectClientTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static final Set<GuidEntry> createdGuids = new HashSet<>();

  /**
   *
   */
  public SelectClientTest() {
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
  public void test_01_testQuerySelect() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry, fieldName, array);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectNear: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_SelectCleanup() {
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
