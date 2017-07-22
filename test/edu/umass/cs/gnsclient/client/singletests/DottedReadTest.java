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
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test read fields with dot notation.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DottedReadTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  /**
   *
   */
  public DottedReadTest() {
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
  public void test_10_CreateFields() {
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
  public void test_20_Update() {
    try {
      JSONObject json = new JSONObject();
      JSONObject subJson = new JSONObject();
      subJson.put("sally", "red");
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      json.put("flapjack", subJson);
      clientCommands.update(westyEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while adding field \"flapjack\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_30_ReadAll() {
    try {
      JSONObject expected = new JSONObject();
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_40_ReadDeep() {
    try {
      String actual = clientCommands.fieldRead(westyEntry.getGuid(), "flapjack.sally.right", westyEntry);
      Assert.assertEquals("seven", actual);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack.sally.right\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_50_ReadMid() {
    try {
      String actual = clientCommands.fieldRead(westyEntry.getGuid(), "flapjack.sally", westyEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack.sally\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_60_ReadShallow() {
    try {
      String actual = clientCommands.fieldRead(westyEntry.getGuid(), "flapjack", westyEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_70_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing test account guid: " + e);
    }
  }

}
