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
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.json.JSONObject;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;

/**
 * JSON User update test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MultiFieldLookupTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  /**
   *
   */
  public MultiFieldLookupTest() {
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
  public void test_01_JSONUpdate() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
    try {
      JSONObject json = new JSONObject();
      json.put("name", "frank");
      json.put("occupation", "busboy");
      json.put("location", "work");
      json.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      json.put("gibberish", subJson);
      clientCommands.update(westyEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while updating JSON: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_MultiFieldLookup() {
    try {
      String actual = clientCommands.fieldRead(westyEntry, new ArrayList<>(Arrays.asList("name", "occupation")));
      JSONAssert.assertEquals("{\"name\":\"frank\",\"occupation\":\"busboy\"}", actual, true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"name\" and \"occupation\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }

}
