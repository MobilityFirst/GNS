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
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import java.io.IOException;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DottedReadTest {

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  public DottedReadTest() {
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
  public void test_1_CreateFields() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_2_Update() {
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
      client.update(westyEntry, json);
    } catch (Exception e) {
      fail("Exception while adding field \"flapjack\": " + e);
    }
  }

  @Test
  public void test_3_ReadAll() {
    try {
      JSONObject expected = new JSONObject();
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

  @Test
  public void test_4_ReadDeep() {
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.sally.right", westyEntry);
      assertEquals("seven", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally.right\": " + e);
    }
  }

  @Test
  public void test_5_ReadMid() {
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.sally", westyEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally\": " + e);
    }
  }

  @Test
  public void test_6_ReadShallow() {
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack", westyEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack\": " + e);
    }
  }

}
