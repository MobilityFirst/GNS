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
import java.util.ArrayList;
import java.util.Arrays;
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
public class NewReadTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client = null;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;

  public NewReadTest() {
    if (client == null) {
      address = ServerSelectDialog.selectServer();
      //System.out.println("Connecting to " + address.getHostName() + ":" + address.getPort());
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }
  
  @Test
  public void test_10_CreateFields() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }
  
  @Test
  public void test_71_JSONUpdate() {
    try {
      JSONObject json = new JSONObject();
      json.put("name", "frank");
      json.put("occupation", "busboy");
      json.put("location", "work");
      json.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      json.put("gibberish", subJson);
      client.update(westyEntry, json);
    } catch (Exception e) {
      fail("Exception while updating JSON: " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "busboy");
      expected.put("location", "work");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      client.update(westyEntry, json);
    } catch (Exception e) {
      fail("Exception while changing \"occupation\" to \"rocket scientist\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading change of \"occupation\" to \"rocket scientist\": " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      client.update(westyEntry, json);
    } catch (Exception e) {
      fail("Exception while adding field \"ip address\" with value \"127.0.0.1\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }

    try {
      client.fieldRemove(westyEntry.getGuid(), "gibberish", westyEntry);
    } catch (Exception e) {
      fail("Exception during remove field \"gibberish\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

  @Test
  public void test_72_NewRead() {
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

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.sally.right", westyEntry);
      assertEquals("seven", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally.right\": " + e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.sally", westyEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally\": " + e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack", westyEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack\": " + e);
    }
  }

  @Test
  public void test_73_NewUpdate() {
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sally.right", "crank", westyEntry);
    } catch (Exception e) {
      fail("Exception while updating field \"flapjack.sally.right\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sammy", new ArrayList(Arrays.asList("One", "Ready", "Frap")), westyEntry);
    } catch (Exception e) {
      fail("Exception while updating field \"flapjack.sammy\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", new ArrayList(Arrays.asList("One", "Ready", "Frap")));
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash", new ArrayList(Arrays.asList("Tango", "Sierra", "Alpha")));
      client.fieldUpdate(westyEntry.getGuid(), "flapjack", moreJson, westyEntry);
    } catch (Exception e) {
      fail("Exception while updating field \"flapjack\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash", new ArrayList(Arrays.asList("Tango", "Sierra", "Alpha")));
      expected.put("flapjack", moreJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

}
