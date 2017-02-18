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
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import java.util.Set;
import org.hamcrest.Matchers;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Tests using multiple clients.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MultipleClientTest extends DefaultGNSTest {

  private static List<GNSClientCommands> multipleClients = null;

  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;
  private static GuidEntry updateEntry;

  private final static Random RANDOM = new Random();

  private GNSClientCommands getRandomClient() {
    return multipleClients.get(RANDOM.nextInt(multipleClients.size()));
  }

  /**
   *
   */
  public MultipleClientTest() {
    if (multipleClients == null) {
      multipleClients = new ArrayList<>();
      try {
        for (int amount = 5; amount > 0; amount--) {
          multipleClients.add(new GNSClientCommands());
        }
      } catch (IOException e) {
        Utils.failWithStackTrace("Unable to create clients: " + e);
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
  public void test_01_CreateEntity() {
    String name = "testGUID" + RandomString.randomString(12);
    GuidEntry guidEntry = null;
    try {
      guidEntry = getRandomClient().guidCreate(masterGuid, name);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating guid: " + e);
    }
    Assert.assertNotNull(guidEntry);
    Assert.assertEquals(name, guidEntry.getEntityName());
    try {
      getRandomClient().guidRemove(masterGuid, guidEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while deleting guid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_RemoveGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = getRandomClient().guidCreate(masterGuid, testGuidName);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }
    if (testGuid != null) {
      try {
        getRandomClient().guidRemove(masterGuid, testGuid.getGuid());
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while removing testGuid: " + e);
      }
      int cnt = 0;
      try {
        do {
          try {
            getRandomClient().lookupGuidRecord(testGuid.getGuid());
            if (cnt++ > 10) {
              Utils.failWithStackTrace(testGuid.getGuid() + " should not exist (after 10 checks)");
              break;
            }
          } catch (IOException e) {
            Utils.failWithStackTrace("Exception while looking up alias: " + e);
          }
          ThreadUtils.sleep(10);
        } while (true);
        // the lookup should Utils.failWithStackTrace and throw to here
      } catch (ClientException e) {
      }
      try {
        getRandomClient().guidRemove(masterGuid, testGuid.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_03_RemoveGuidSansAccountInfo() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = getRandomClient().guidCreate(masterGuid, testGuidName);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }
    if (testGuid != null) {
      try {
        getRandomClient().guidRemove(testGuid);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while removing testGuid: " + e);
      }
      try {
        getRandomClient().lookupGuidRecord(testGuid.getGuid());
        Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
      } catch (ClientException e) {

      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
      }
      try {
        getRandomClient().guidRemove(masterGuid, testGuid.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_04_LookupPrimaryGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = getRandomClient().guidCreate(masterGuid, testGuidName);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }
    if (testGuid != null) {
      try {
        Assert.assertEquals(masterGuid.getGuid(), getRandomClient().lookupPrimaryGuid(testGuid.getGuid()));
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while looking up primary guid for testGuid: " + e);
      }
      try {
        getRandomClient().guidRemove(masterGuid, testGuid.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_05_CreateSubGuid() {
    try {
      subGuidEntry = getRandomClient().guidCreate(masterGuid, "subGuid" + RandomString.randomString(12));
      System.out.println("Created: " + subGuidEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_06_FieldNotFoundException() {
    try {
      getRandomClient().fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry);
      Utils.failWithStackTrace("Should have thrown an exception.");
    } catch (FieldNotFoundException e) {
      System.out.println("This was expected: " + e);
    } catch (IOException | ClientException e) {
      System.out.println("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_07_FieldExistsFalse() {
    try {
      Assert.assertFalse(getRandomClient().fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (IOException | ClientException e) {
      System.out.println("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_08_CreateFieldForFieldExists() {
    try {
      getRandomClient().fieldCreateOneElementList(subGuidEntry.getGuid(), "environment", "work", subGuidEntry);
    } catch (IOException | ClientException e) {

      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_09_FieldExistsTrue() {
    try {
      Assert.assertTrue(getRandomClient().fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (IOException | ClientException e) {
      System.out.println("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_10_CreateFields() {
    try {
      westyEntry = getRandomClient().guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = getRandomClient().guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
    try {
      // remove default read acces for this test
      getRandomClient().aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);

      // read my own field
      Assert.assertEquals("work",
              getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another field
      Assert.assertEquals("000-00-0000",
              getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));

      try {
        String result = getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
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
  public void test_11_ACLPartOne() {
    //testCreateField();

    try {
      System.out.println("Using:" + westyEntry);
      System.out.println("Using:" + samEntry);
      try {
        getRandomClient().aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment",
                samEntry.getGuid());
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception adding Sam to Westy's readlist: " + e);

      }
      try {
        Assert.assertEquals("work",
                getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Sam reading Westy's field: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_12_ACLPartTwo() {
    try {
      String barneyName = "barney" + RandomString.randomString(12);
      try {
        getRandomClient().lookupGuid(barneyName);
        Utils.failWithStackTrace(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception looking up Barney: " + e);

      }
      barneyEntry = getRandomClient().guidCreate(masterGuid, barneyName);
      // remove default read access for this test
      getRandomClient().aclRemove(AclAccessType.READ_WHITELIST, barneyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      getRandomClient().fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      getRandomClient().fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);

      try {
        // let anybody read barney's cell field
        getRandomClient().aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception creating ALLUSERS access for Barney's cell: " + e);

      }

      try {
        Assert.assertEquals("413-555-1234",
                getRandomClient().fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", samEntry));
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Sam reading Barney' cell: " + e);

      }

      try {
        Assert.assertEquals("413-555-1234",
                getRandomClient().fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Westy reading Barney' cell: " + e);

      }

      try {
        String result = getRandomClient().fieldReadArrayFirstElement(barneyEntry.getGuid(), "address",
                samEntry);
        Utils.failWithStackTrace("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        // normal result
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while Sam reading Barney' address: " + e);

      }

    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_13_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(12);
    try {
      try {
        getRandomClient().lookupGuid(superUserName);
        Utils.failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = getRandomClient().guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      getRandomClient().aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      Assert.assertEquals("413-555-1234",
              getRandomClient().fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      Assert.assertEquals("100 Main Street",
              getRandomClient().fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));
      try {
        getRandomClient().guidRemove(masterGuid, superuserEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
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
    //testCreateEntity();
    try {

      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), "cats", "whacky", westyEntry);

      Assert.assertEquals("whacky",
              getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

      getRandomClient().fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("hooch", "maya", "red", "sox", "toby")), westyEntry);

      HashSet<String> expected = new HashSet<>(Arrays.asList("hooch",
              "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      getRandomClient().fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      getRandomClient().fieldReplaceFirstElement(westyEntry.getGuid(), "cats", "maya", westyEntry);
      Assert.assertEquals("maya",
              getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

//      try {
//        getRandomClient().fieldCreateOneElementList(westyEntry, "cats", "maya");
//        Utils.failWithStackTrace("Should have got an exception when trying to create the field westy / cats.");
//      } catch (ClientException e) {
//      }
      //this one always Utils.failWithStackTraces... check it out
//      try {
//        getRandomClient().fieldAppendWithSetSemantics(westyEntry.getGuid(), "frogs", "freddybub",
//                westyEntry);
//        Utils.failWithStackTrace("Should have got an exception when trying to create the field westy / frogs.");
//      } catch (ClientException e) {
//      }
      getRandomClient().fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      getRandomClient().fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
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

      getRandomClient().fieldAppendOrCreate(westyEntry.getGuid(), "dogs", "bear", westyEntry);
      expected = new HashSet<>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("1) Looking for bear: " + e);
    }
    try {
      getRandomClient().fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);
      expected = new HashSet<>(Arrays.asList("bear", "wags", "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("2) Looking for bear, wags, tucker: " + e);
    }
    try {
      getRandomClient().fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue", westyEntry);
      expected = new HashSet<>(Arrays.asList("sue"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("3) Looking for sue: " + e);
    }
    try {
      getRandomClient().fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "william", westyEntry);
      expected = new HashSet<>(Arrays.asList("william"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("4) Looking for william: " + e);
    }
    try {
      getRandomClient().fieldReplaceOrCreateList(westyEntry.getGuid(), "goats",
              new JSONArray(Arrays.asList("dink", "tink")), westyEntry);
      expected = new HashSet<>(Arrays.asList("dink", "tink"));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(
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
      testEntry = getRandomClient().guidCreate(masterGuid, testSubstituteGuid);
      System.out.println("created test guid: " + testEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during init: " + e);
    }
    if (testEntry != null) {
      try {
        getRandomClient().fieldAppendOrCreateList(testEntry.getGuid(), field, new JSONArray(Arrays.asList("Frank", "Joe", "Sally", "Rita")), testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during create: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("Frank", "Joe", "Sally", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }

      try {
        getRandomClient().fieldSubstitute(testEntry.getGuid(), field, "Christy", "Sally", testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during substitute: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("Frank", "Joe", "Christy", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
      try {
        getRandomClient().guidRemove(masterGuid, testEntry.getGuid());
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
      testEntry = getRandomClient().guidCreate(masterGuid, testSubstituteListGuid);
      System.out.println("created test guid: " + testEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during init: " + e);
    }
    if (testEntry != null) {
      try {
        getRandomClient().fieldAppendOrCreateList(testEntry.getGuid(), field, new JSONArray(Arrays.asList("Frank", "Joe", "Sally", "Rita")), testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during create: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("Frank", "Joe", "Sally", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }

      try {
        getRandomClient().fieldSubstitute(testEntry.getGuid(), field,
                new JSONArray(Arrays.asList("BillyBob", "Hank")),
                new JSONArray(Arrays.asList("Frank", "Joe")),
                testEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception during substitute: " + e);
      }

      try {
        HashSet<String> expected = new HashSet<>(Arrays.asList("BillyBob", "Hank", "Sally", "Rita"));
        HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().fieldReadArray(testEntry.getGuid(), field, testEntry));
        Assert.assertEquals(expected, actual);
      } catch (IOException | ClientException | JSONException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
      try {
        getRandomClient().guidRemove(masterGuid, testEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_18_Group() {
    String mygroupName = "mygroup" + RandomString.randomString(12);
    try {
      try {
        getRandomClient().lookupGuid(mygroupName);
        Utils.failWithStackTrace(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = getRandomClient().guidCreate(masterGuid, "deleteMe" + RandomString.randomString(12));
      mygroupEntry = getRandomClient().guidCreate(masterGuid, mygroupName);

      getRandomClient().groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      getRandomClient().groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(), mygroupEntry);
      getRandomClient().groupAddGuid(mygroupEntry.getGuid(), guidToDeleteEntry.getGuid(), mygroupEntry);

      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

      expected = new HashSet<>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(getRandomClient().guidGetGroups(westyEntry.getGuid(), westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      System.exit(2);
    }
    // now remove a guid and check for group updates
    try {
      getRandomClient().guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
    try {
      getRandomClient().lookupGuidRecord(guidToDeleteEntry.getGuid());
      Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }

    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception during remove guid group update test: " + e);
      System.exit(2);
    }

  }

  /**
   *
   */
  @Test
  public void test_19_GroupAndACL() {
    //testGroup();
    String groupAccessUserName = "groupAccessUser" + RandomString.randomString(12);
    try {
      try {
        getRandomClient().lookupGuid(groupAccessUserName);
        Utils.failWithStackTrace(groupAccessUserName + " entity should not exist");
      } catch (ClientException e) {
      }
    } catch (IOException e) {
      Utils.failWithStackTrace("Checking for existence of group user: " + e);
    }
    GuidEntry groupAccessUserEntry;

    try {
      groupAccessUserEntry = getRandomClient().guidCreate(masterGuid, groupAccessUserName);
      // remove all fields read by all
      getRandomClient().aclRemove(AclAccessType.READ_WHITELIST, groupAccessUserEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception creating group user: " + e);
      return;
    }

    try {
      getRandomClient().fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "address", "23 Jumper Road", groupAccessUserEntry);
      getRandomClient().fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "age", "43", groupAccessUserEntry);
      getRandomClient().fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "hometown", "whoville", groupAccessUserEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception creating group user fields: " + e);
      return;
    }
    try {
      getRandomClient().aclAdd(AclAccessType.READ_WHITELIST, groupAccessUserEntry, "hometown", mygroupEntry.getGuid());
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception adding mygroup to acl for group user hometown field: " + e);
      return;
    }
    try {
      try {
        String result = getRandomClient().fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "address", westyEntry);
        Utils.failWithStackTrace("Result of read of groupAccessUser's age by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while attempting a Utils.failWithStackTraceing read of groupAccessUser's age by sam: " + e);
    }
    try {
      Assert.assertEquals("whoville", getRandomClient().fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while attempting read of groupAccessUser's hometown by westy: " + e);
    }
    try {
      try {
        getRandomClient().groupRemoveGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception removing westy from mygroup: " + e);
        return;
      }

      HashSet<String> expected = new HashSet<>(Arrays.asList(samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
    try {
      getRandomClient().guidRemove(masterGuid, groupAccessUserEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while deleting guid: " + e);
    }
  }

  private static final String alias = "ALIAS-" + RandomString.randomString(12) + "@blah.org";

  /**
   *
   */
  @Test
  public void test_20_AliasAdd() {
    try {
      //
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      //
      // add an alias to the masterGuid
      getRandomClient().addAlias(masterGuid, alias);
      // lookup the guid using the alias
      Assert.assertEquals(masterGuid.getGuid(), getRandomClient().lookupGuid(alias));

      // grab all the aliases from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(getRandomClient().getAliases(masterGuid));
      // make sure our new one is in there
      Assert.assertThat(actual, Matchers.hasItem(alias));

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_21_AliasRemove() {
    try {
      // now remove it 
      getRandomClient().removeAlias(masterGuid, alias);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while removing alias: " + e);
    }
    try {
      getRandomClient().lookupGuid(alias);
      System.out.println(alias + " should not exist");
    } catch (ClientException e) {
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while looking up alias: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_31_BasicSelect() {
    try {
      JSONArray result = getRandomClient().select(masterGuid, "cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_32_GeoSpatialSelect() {
    Set<GuidEntry> createdGuids = new HashSet<>();
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = getRandomClient().guidCreate(masterGuid, "geoTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry);
        getRandomClient().setLocation(testEntry, 0.0, 0.0);
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
    waitSettle(500);
    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = getRandomClient().selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectNear: " + e);
    }

    try {

      JSONArray rect = new JSONArray();
      JSONArray upperLeft = new JSONArray();
      upperLeft.put(1.0);
      upperLeft.put(1.0);
      JSONArray lowerRight = new JSONArray();
      lowerRight.put(-1.0);
      lowerRight.put(-1.0);
      rect.put(upperLeft);
      rect.put(lowerRight);
      JSONArray result = getRandomClient().selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectWithin: " + e);
    }
    try {
      for (GuidEntry guid : createdGuids) {
        getRandomClient().guidRemove(masterGuid, guid.getGuid());
      }
      createdGuids.clear();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_33_QuerySelect() {
    Set<GuidEntry> createdGuids = new HashSet<>();
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = getRandomClient().guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry);
        JSONArray array = new JSONArray(Arrays.asList(25));
        getRandomClient().fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry);
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while tryint to create the guids: " + e);
    }
    waitSettle(500);
    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = getRandomClient().selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectNear: " + e);
    }
    try {
      for (GuidEntry guid : createdGuids) {
        getRandomClient().guidRemove(masterGuid, guid.getGuid());
      }
      createdGuids.clear();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
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
        getRandomClient().aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, fieldName, samEntry.getGuid());
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception adding Sam to Westy's writelist: " + e);

      }
      // write my own field
      try {
        getRandomClient().fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "shopping", westyEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Westy's writing own field: " + e);

      }
      // now check the value
      Assert.assertEquals("shopping", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), fieldName, westyEntry));
      // someone else write my field
      try {
        getRandomClient().fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "driving", samEntry);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while Sam writing Westy's field: " + e);

      }
      // now check the value
      Assert.assertEquals("driving", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), fieldName, westyEntry));
      // do one that should Utils.failWithStackTrace
      try {
        getRandomClient().fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "driving", barneyEntry);
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
  public void test_45_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), unsignedReadFieldName, "funkadelicread", westyEntry);
      getRandomClient().aclAdd(AclAccessType.READ_WHITELIST, westyEntry, unsignedReadFieldName, GNSProtocol.ALL_GUIDS.toString());
      Assert.assertEquals("funkadelicread", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedReadFieldName, null));

      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), standardReadFieldName, "bummer", westyEntry);
      // already did this above... doing it again gives us a paxos error
      //getRandomClient().removeFromACL(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      try {
        String result = getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), standardReadFieldName, null);
        Utils.failWithStackTrace("Result of read of westy's " + standardReadFieldName + " as world readable was " + result
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
  public void test_46_UnsignedWrite() {
    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      getRandomClient().aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, unsignedWriteFieldName, GNSProtocol.ALL_GUIDS.toString());
      getRandomClient().fieldReplaceFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      Assert.assertEquals("funkadelicwrite", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), standardWriteFieldName, "bummer", westyEntry);
      try {
        getRandomClient().fieldReplaceFirstElement(westyEntry.getGuid(), standardWriteFieldName, "funkadelicwrite", null);
        Utils.failWithStackTrace("Write of westy's field " + standardWriteFieldName + " as world readable should have been rejected.");
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
  public void test_47_RemoveField() {
    String fieldToDelete = "fieldToDelete";
    try {
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), fieldToDelete, "work", westyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating the field: " + e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), fieldToDelete, westyEntry));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while reading the field " + fieldToDelete + ": " + e);
    }
    try {
      getRandomClient().fieldRemove(westyEntry.getGuid(), fieldToDelete, westyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while removing field: " + e);
    }

    try {
      String result = getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), fieldToDelete, westyEntry);
      Utils.failWithStackTrace("Result of read of westy's " + fieldToDelete + " is " + result
              + " which is wrong because it should have been deleted.");
    } catch (ClientException e) {
      // normal result
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while removing field: " + e);
    }

  }

  /**
   *
   */
  @Test
  public void test_48_ListOrderAndSetElement() {
    try {

      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), "numbers", "one", westyEntry);

      Assert.assertEquals("one", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), "numbers", westyEntry));

      getRandomClient().fieldAppend(westyEntry.getGuid(), "numbers", "two", westyEntry);
      getRandomClient().fieldAppend(westyEntry.getGuid(), "numbers", "three", westyEntry);
      getRandomClient().fieldAppend(westyEntry.getGuid(), "numbers", "four", westyEntry);
      getRandomClient().fieldAppend(westyEntry.getGuid(), "numbers", "five", westyEntry);

      List<String> expected = new ArrayList<>(Arrays.asList("one", "two", "three", "four", "five"));
      ArrayList<String> actual = JSONUtils.JSONArrayToArrayList(getRandomClient().fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

      getRandomClient().fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2, westyEntry);

      expected = new ArrayList<>(Arrays.asList("one", "two", "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(getRandomClient().fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
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
      getRandomClient().fieldCreateOneElementList(westyEntry.getGuid(), field, "work", westyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating the field: " + e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), field, westyEntry));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while reading the field " + field + ": " + e);
    }
    try {
      getRandomClient().fieldSetNull(westyEntry.getGuid(), field, westyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while setting field to null field: " + e);
    }

    try {
      Assert.assertEquals(null, getRandomClient().fieldReadArrayFirstElement(westyEntry.getGuid(), field, westyEntry));
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
      updateEntry = getRandomClient().guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      System.out.println("Created: " + updateEntry);
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
      getRandomClient().update(updateEntry, json);
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
      JSONObject actual = getRandomClient().read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      getRandomClient().update(updateEntry, json);
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
      JSONObject actual = getRandomClient().read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading change of \"occupation\" to \"rocket scientist\": " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      getRandomClient().update(updateEntry, json);
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
      JSONObject actual = getRandomClient().read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }

    try {
      getRandomClient().fieldRemove(updateEntry.getGuid(), "gibberish", updateEntry);
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
      JSONObject actual = getRandomClient().read(updateEntry);
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
      getRandomClient().update(updateEntry, json);
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
      JSONObject actual = getRandomClient().read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
    try {
      String actual = getRandomClient().fieldRead(updateEntry.getGuid(), "flapjack.sally.right", updateEntry);
      Assert.assertEquals("seven", actual);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack.sally.right\": " + e);
    }
    try {
      String actual = getRandomClient().fieldRead(updateEntry.getGuid(), "flapjack.sally", updateEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //String expected = "{\"left\":\"eight\",\"right\":\"seven\"}";
      JSONAssert.assertEquals(expected, actual, true);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading \"flapjack.sally\": " + e);
    }
    try {
      String actual = getRandomClient().fieldRead(updateEntry.getGuid(), "flapjack", updateEntry);
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
      getRandomClient().fieldUpdate(updateEntry.getGuid(), "flapjack.sally.right", "crank", updateEntry);
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
      JSONObject actual = getRandomClient().read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading JSON: " + e);
    }
    try {
      getRandomClient().fieldUpdate(updateEntry.getGuid(), "flapjack.sammy", new ArrayList<>(Arrays.asList("One", "Ready", "Frap")), updateEntry);
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
      JSONObject actual = getRandomClient().read(updateEntry);
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
      getRandomClient().fieldUpdate(updateEntry.getGuid(), "flapjack", moreJson, updateEntry);
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
      JSONObject actual = getRandomClient().read(updateEntry);
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
      String actual = getRandomClient().fieldRead(updateEntry, new ArrayList<>(Arrays.asList("name", "occupation")));
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
      getRandomClient().guidRemove(masterGuid, westyEntry.getGuid());
      getRandomClient().guidRemove(masterGuid, samEntry.getGuid());
      getRandomClient().guidRemove(masterGuid, barneyEntry.getGuid());
      getRandomClient().guidRemove(masterGuid, mygroupEntry.getGuid());
      getRandomClient().guidRemove(masterGuid, subGuidEntry.getGuid());
      getRandomClient().guidRemove(masterGuid, updateEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }

  private static void waitSettle(long wait) {
    try {
      if (wait > 0) {
        Thread.sleep(wait);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
