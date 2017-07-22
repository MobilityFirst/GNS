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
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Tests different data commands.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;

  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;
  private static GuidEntry updateEntry;

  /**
   *
   */
  public DatabaseTest() {
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
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      barneyEntry = clientCommands.guidCreate(masterGuid, "barney" + RandomString.randomString(12));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
    try {
      // remove default read acces for this test
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);

      // read my own field
      Assert.assertEquals("work",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another field
      Assert.assertEquals("000-00-0000",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));

      try {
        String result = clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
                samEntry);
        Utils.failWithStackTrace("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  
  /**
   *
   */
  @Test
  public void test_14_DB() {
    try {

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "cats", "whacky", westyEntry);

      Assert.assertEquals("whacky",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("hooch", "maya", "red", "sox", "toby")), westyEntry);

      HashSet<String> expected = new HashSet<>(Arrays.asList("hooch",
              "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), "cats", "maya", westyEntry);
      Assert.assertEquals("maya",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

//      try {
//        client.fieldCreateOneElementList(westyEntry, "cats", "maya");
//        Utils.failWithStackTrace("Should have got an exception when trying to create the field westy / cats.");
//      } catch (ClientException e) {
//      }
      //this one always Utils.failWithStackTraces... check it out
//      try {
//        client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "frogs", "freddybub",
//                westyEntry);
//        Utils.failWithStackTrace("Should have got an exception when trying to create the field westy / frogs.");
//      } catch (ClientException e) {
//      }
      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_15_DBUpserts() {
    HashSet<String> expected;
    HashSet<String> actual;
    try {

      clientCommands.fieldAppendOrCreate(westyEntry.getGuid(), "dogs", "bear", westyEntry);
      expected = new HashSet<>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("1) Looking for bear: " + e);
    }
    try {
      clientCommands.fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);
      expected = new HashSet<>(Arrays.asList("bear", "wags", "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("2) Looking for bear, wags, tucker: " + e);
    }
    try {
      clientCommands.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue", westyEntry);
      expected = new HashSet<>(Arrays.asList("sue"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("3) Looking for sue: " + e);
    }
    try {
      clientCommands.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "william", westyEntry);
      expected = new HashSet<>(Arrays.asList("william"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("4) Looking for william: " + e);
    }
    try {
      clientCommands.fieldReplaceOrCreateList(westyEntry.getGuid(), "goats",
              new JSONArray(Arrays.asList("dink", "tink")), westyEntry);
      expected = new HashSet<>(Arrays.asList("dink", "tink"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("5) Looking for dink, tink: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_16_Substitute() {
    String testSubstituteGuid = "testSubstituteGUID" + RandomString.randomString(12);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      testEntry = clientCommands.guidCreate(masterGuid, testSubstituteGuid);
      //System.out.println("created test guid: " + testEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during init: " + e);
    }
    if (testEntry != null) {
      try {
        clientCommands.fieldAppendOrCreateList(testEntry.getGuid(), field, new JSONArray(Arrays.asList("Frank", "Joe", "Sally", "Rita")), testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during create: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("Frank", "Joe", "Sally", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }

      try {
        clientCommands.fieldSubstitute(testEntry.getGuid(), field, "Christy", "Sally", testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during substitute: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("Frank", "Joe", "Christy", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
      try {
        clientCommands.guidRemove(masterGuid, testEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_17_SubstituteList() {
    String testSubstituteListGuid = "testSubstituteListGUID" + RandomString.randomString(12);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      //Utils.clearTestGuids(client);
      //System.out.println("cleared old GUIDs");
      testEntry = clientCommands.guidCreate(masterGuid, testSubstituteListGuid);
      //System.out.println("created test guid: " + testEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during init: " + e);
    }
    if (testEntry != null) {
      try {
        clientCommands.fieldAppendOrCreateList(testEntry.getGuid(), field, new JSONArray(Arrays.asList("Frank", "Joe", "Sally", "Rita")), testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during create: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("Frank", "Joe", "Sally", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }

      try {
        clientCommands.fieldSubstitute(testEntry.getGuid(), field,
                new JSONArray(Arrays.asList("BillyBob", "Hank")),
                new JSONArray(Arrays.asList("Frank", "Joe")),
                testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during substitute: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("BillyBob", "Hank", "Sally", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
      try {
        clientCommands.guidRemove(masterGuid, testEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_44_WriteAccess() {
    String fieldName = "whereAmI";
    try {
      try {
        clientCommands.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, fieldName, samEntry.getGuid());
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception adding Sam to Westy's writelist: " + e);

      }
      // write my own field
      try {
        clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "shopping", westyEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Westy's writing own field: " + e);

      }
      // now check the value
      Assert.assertEquals("shopping", clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldName, westyEntry));
      // someone else write my field
      try {
        clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "driving", samEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Sam writing Westy's field: " + e);

      }
      // now check the value
      Assert.assertEquals("driving", clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldName, westyEntry));
      // do one that should Utils.failWithStackTrace
      try {
        clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "driving", barneyEntry);
        Utils.failWithStackTrace("Write by barney should have Utils.failWithStackTraceed!");
      } catch (ClientException e) {
        // normal result
      } catch (IOException e) {

        Utils.failWithStackTrace("Exception during read of westy's " + fieldName + " by sam: " + e);
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_48_ListOrderAndSetElement() {
    try {

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "numbers", "one", westyEntry);

      Assert.assertEquals("one", clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "numbers", westyEntry));

      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "two", westyEntry);
      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "three", westyEntry);
      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "four", westyEntry);
      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "five", westyEntry);

      List<String> expected = new ArrayList<>(Arrays.asList("one", "two", "three", "four", "five"));
      ArrayList<String> actual = JSONUtils.JSONArrayToArrayList(clientCommands.fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2, westyEntry);

      expected = new ArrayList<>(Arrays.asList("one", "two", "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(clientCommands.fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Unexpected exception during test: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_70_SetFieldNull() {
    String field = "fieldToSetToNull";
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), field, "work", westyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating the field: " + e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), field, westyEntry));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while reading the field " + field + ": " + e);
    }
    try {
      clientCommands.fieldSetNull(westyEntry.getGuid(), field, westyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while setting field to null field: " + e);
    }

    try {
      Assert.assertEquals(null, clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), field, westyEntry));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while reading the field " + field + ": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_71_JSONUpdate() {
    try {
      updateEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      //System.out.println("Created: " + updateEntry);
    } catch (IOException | ClientException e) {
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
      clientCommands.update(updateEntry, json);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while updating JSON: " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "busboy");
      expected.put("location", "work");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      clientCommands.update(updateEntry, json);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while changing \"occupation\" to \"rocket scientist\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading change of \"occupation\" to \"rocket scientist\": " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      clientCommands.update(updateEntry, json);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while adding field \"ip address\" with value \"127.0.0.1\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }

    try {
      clientCommands.fieldRemove(updateEntry.getGuid(), "gibberish", updateEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during remove field \"gibberish\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
  }

  /**
   *
   */
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
      clientCommands.update(updateEntry, json);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while adding field \"flapjack\": " + e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
    try {
      String actual = clientCommands.fieldRead(updateEntry.getGuid(), "flapjack.sally.right", updateEntry);
      Assert.assertEquals("seven", actual);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack.sally.right\": " + e);
    }
    try {
      String actual = clientCommands.fieldRead(updateEntry.getGuid(), "flapjack.sally", updateEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //String expected = "{\"left\":\"eight\",\"right\":\"seven\"}";
      JSONAssert.assertEquals(expected, actual, true);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack.sally\": " + e);
    }
    try {
      String actual = clientCommands.fieldRead(updateEntry.getGuid(), "flapjack", updateEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      //String expected = "{\"sammy\":\"green\",\"sally\":{\"left\":\"eight\",\"right\":\"seven\"}}";
      JSONAssert.assertEquals(expected, actual, true);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_73_NewUpdate() {
    try {
      clientCommands.fieldUpdate(updateEntry.getGuid(), "flapjack.sally.right", "crank", updateEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while updating field \"flapjack.sally.right\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
    try {
      clientCommands.fieldUpdate(updateEntry.getGuid(), "flapjack.sammy", new ArrayList<>(Arrays.asList("One", "Ready", "Frap")), updateEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while updating field \"flapjack.sammy\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", new ArrayList<>(Arrays.asList("One", "Ready", "Frap")));
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
    try {
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash", new ArrayList<>(Arrays.asList("Tango", "Sierra", "Alpha")));
      clientCommands.fieldUpdate(updateEntry.getGuid(), "flapjack", moreJson, updateEntry);
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while updating field \"flapjack\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash", new ArrayList<>(Arrays.asList("Tango", "Sierra", "Alpha")));
      expected.put("flapjack", moreJson);
      JSONObject actual = clientCommands.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_74_MultiFieldLookup() {
    try {
      String actual = clientCommands.fieldRead(updateEntry, new ArrayList<>(Arrays.asList("name", "occupation")));
      JSONAssert.assertEquals("{\"name\":\"frank\",\"occupation\":\"rocket scientist\"}", actual, true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"name\" and \"occupation\": " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_80_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
      clientCommands.guidRemove(masterGuid, samEntry.getGuid());
      clientCommands.guidRemove(masterGuid, barneyEntry.getGuid());
      clientCommands.guidRemove(masterGuid, updateEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }
}
