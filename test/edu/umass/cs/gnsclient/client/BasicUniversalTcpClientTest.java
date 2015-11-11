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

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import java.net.InetSocketAddress;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;

/**
 * JSON User update test for the GNS.
 *
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicUniversalTcpClientTest {

  private static final String ACCOUNT_ALIAS = "david@westy.org"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static BasicUniversalTcpClient client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;

  
  public BasicUniversalTcpClientTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new BasicUniversalTcpClient(address.getHostName(), address.getPort());
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void test_1_JSONUpdate() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
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
      System.out.println(actual);
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
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception reading change of \"occupation\" to \"rocket scientist\": " + e);
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
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }

    try {
      client.fieldRemove(westyEntry.getGuid(), "gibberish", westyEntry);
    } catch (Exception e) {
      fail("Exception remove field \"gibberish\": " + e);
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
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

  @Test
  public void test_2_NewRead() {
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
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack", westyEntry);
      assertEquals("{\"sammy\":\"green\",\"sally\":{\"left\":\"eight\",\"right\":\"seven\"}}", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack\": " + e);
    }
     try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.sally", westyEntry);
      assertEquals("{\"left\":\"eight\",\"right\":\"seven\"}", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally\": " + e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.sally.right", westyEntry);
      assertEquals("seven", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally.right\": " + e);
    }
   
    
  }

  @Test
  public void test_3_NewUpdate() {
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
      System.out.println(actual);
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
      System.out.println(actual);
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
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }
  
  @Test
  public void test_4_HierarchicalACL() {
    
    try {
      client.aclRemove(GnsProtocol.AccessType.READ_WHITELIST, westyEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
    } catch (Exception e) {
      fail("Exception while removing access for all users to all fields: " + e);
    }
    
    try {
      samEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception while creating Sam: " + e);
    }
    
    try {
      client.aclAdd(GnsProtocol.AccessType.READ_WHITELIST, westyEntry, "flapjack.crash", samEntry.getGuid());
    } catch (Exception e) {
      fail("Exception while adding access for sam to \"flapjack.crash\": " + e);
    }
    
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.crash", samEntry);
      assertEquals(new JSONArray(Arrays.asList("Tango", "Sierra", "Alpha")).toString(), actual);
    } catch (Exception e) {
      fail("Exception while sam reading \"flapjack.crash\": " + e);
    }
    
    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack.shattered", samEntry);
      fail("Should not have been able to read \"flapjack.shattered\"");
    } catch (Exception e) {
    }
    
    try {
      client.aclAdd(GnsProtocol.AccessType.READ_WHITELIST, westyEntry, "flapjack", samEntry.getGuid());
    } catch (Exception e) {
      fail("Exception while adding access for sam to \"flapjack\": " + e);
    }
  }

}
