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
package edu.umass.cs.gnsclient.client.integrationtests;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.utils.Base64;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;

import static org.hamcrest.Matchers.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * THIS HAS BEEN REPLACED WITH ServerIntegrationTest class
 *
 * Functionality test for core elements in the client using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClientCoreTest {

  private static String accountAlias = "test@cgns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static String password = "password";
  private static  GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;

  private static final int COORDINATION_WAIT = 100;

  private static void waitSettle() {
    try {
      Thread.sleep(COORDINATION_WAIT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public ClientCoreTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands(null);
        //client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      if (System.getProperty("alias") != null
              && !System.getProperty("alias").isEmpty()) {
        accountAlias = System.getProperty("alias");
      }
      if (System.getProperty("password") != null
              && !System.getProperty("password").isEmpty()) {
        password = System.getProperty("password");
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, accountAlias, password, true);
      } catch (Exception e) {
        fail("Exception while creating account guid: " + e);
      }
    }
  }

  @Test
  public void test_010_CreateEntity() {
    String localAlias = "testGUID" + RandomString.randomString(6);
    GuidEntry guidEntry = null;
    try {
      guidEntry = client.guidCreate(masterGuid, localAlias);
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
    assertNotNull(guidEntry);
    assertEquals(localAlias, guidEntry.getEntityName());
  }

  @Test
  public void test_020_RemoveGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: " + e);
    }
    try {
      client.guidRemove(masterGuid, testGuid.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  @Test
  public void test_030_RemoveGuidSansAccountInfo() {
    String testGuidName = "testGUID" + RandomString.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: " + e);
    }
    try {
      client.guidRemove(testGuid);
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  @Test
  public void test_040_LookupPrimaryGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: " + e);
    }
    try {
      assertEquals(masterGuid.getGuid(), client.lookupPrimaryGuid(testGuid.getGuid()));
    } catch (IOException | ClientException e) {
      fail("Exception while looking up primary guid for testGuid: " + e);
    }
  }

  @Test
  public void test_050_CreateSubGuid() {
    try {
      subGuidEntry = client.guidCreate(masterGuid, "subGuid" + RandomString.randomString(6));
      System.out.println("Created: " + subGuidEntry);
    } catch (Exception e) {
      fail("Exception creating subguid: " + e);
    }
  }

  @Test
  public void test_060_FieldNotFoundException() {
    try {
      client.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry);
      fail("Should have thrown an exception.");
    } catch (FieldNotFoundException e) {
      System.out.println("This was expected: " + e);
    } catch (Exception e) {
      System.out.println("Exception testing field not found: " + e);
    }
  }

  @Test
  public void test_070_FieldExistsFalse() {
    try {
      assertFalse(client.fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (Exception e) {
      System.out.println("Exception testing field exists false: " + e);
    }
  }

  @Test
  public void test_080_CreateFieldForFieldExists() {
    try {
      client.fieldCreateOneElementList(subGuidEntry.getGuid(), "environment", "work", subGuidEntry);
    } catch (IOException | ClientException e) {
      e.printStackTrace();
      fail("Exception during create field: " + e);
    }
  }

  @Test
  public void test_090_FieldExistsTrue() {
    try {
      assertTrue(client.fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (Exception e) {
      System.out.println("Exception testing field exists true: " + e);
    }
  }

  @Test
  public void test_100_CreateFields() {
    try {
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
      samEntry = client.guidCreate(masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception registering guids for create fields: " + e);
    }
    try {
      // remove default read acces for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSCommandProtocol.ALL_FIELDS, GNSCommandProtocol.ALL_GUIDS);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);

      // read my own field
      assertEquals("work",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another field
      assertEquals("000-00-0000",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));

      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
                samEntry);
        fail("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
       e.printStackTrace();
      fail("Exception when we were not expecting it in create fields: " + e);
    }
  }

  @Test
  public void test_110_ACLPartOne() {
    //testCreateField();

    try {
      System.out.println("Using:" + westyEntry);
      System.out.println("Using:" + samEntry);
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment",
                samEntry.getGuid());
      } catch (Exception e) {
        fail("Exception adding Sam to Westy's readlist: " + e);
        e.printStackTrace();
      }
      try {
        assertEquals("work",
                client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
      } catch (Exception e) {
        fail("Exception while Sam reading Westy's field: " + e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it testing ACL part one: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_120_ACLPartTwo() {
    try {
      String barneyName = "barney" + RandomString.randomString(6);
      try {
        client.lookupGuid(barneyName);
        fail(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (Exception e) {
        fail("Exception looking up Barney: " + e);
        e.printStackTrace();
      }
      barneyEntry = client.guidCreate(masterGuid, barneyName);
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry, GNSCommandProtocol.ALL_FIELDS, GNSCommandProtocol.ALL_GUIDS);
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);

      try {
        // let anybody read barney's cell field
        client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSCommandProtocol.ALL_GUIDS);
      } catch (Exception e) {
        fail("Exception creating ALLUSERS access for Barney's cell: " + e);
        e.printStackTrace();
      }

      try {
        assertEquals("413-555-1234",
                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", samEntry));
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' cell: " + e);
        e.printStackTrace();
      }

      try {
        assertEquals("413-555-1234",
                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (Exception e) {
        fail("Exception while Westy reading Barney' cell: " + e);
        e.printStackTrace();
      }

      try {
        String result = client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address",
                samEntry);
        fail("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' address: " + e);
        e.printStackTrace();
      }

    } catch (Exception e) {
      fail("Exception when we were not expecting it testing ACL part two: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_130_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        fail(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = client.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSCommandProtocol.ALL_FIELDS, superuserEntry.getGuid());

      assertEquals("413-555-1234",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      fail("Exception when we were not expecting it testing ACL all fields: " + e);
    }
  }

  @Test
  public void test_140_ACLCreateDeeperField() {
    try {
      try {
        client.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (Exception e) {
        fail("Problem updating field: " + e);
      }
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field", GNSCommandProtocol.ALL_FIELDS);
      } catch (Exception e) {
        fail("Problem adding acl: " + e);
      }
      try {
        JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                "test.deeper.field", westyEntry.getGuid());
        JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(GNSCommandProtocol.ALL_FIELDS)));
        JSONAssert.assertEquals(expected, actual, true);
      } catch (Exception e) {
        fail("Problem reading acl: " + e);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_170_DB() {
    //testCreateEntity();
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "cats", "whacky", westyEntry);

      assertEquals("whacky",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("hooch", "maya", "red", "sox", "toby")), westyEntry);

      HashSet<String> expected = new HashSet<>(Arrays.asList("hooch",
              "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldReplaceFirstElement(westyEntry.getGuid(), "cats", "maya", westyEntry);
      assertEquals("maya",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

//      try {
//        client.fieldCreateOneElementList(westyEntry, "cats", "maya");
//        fail("Should have got an exception when trying to create the field westy / cats.");
//      } catch (ClientException e) {
//      }
      //this one always fails... check it out
//      try {
//        client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "frogs", "freddybub",
//                westyEntry);
//        fail("Should have got an exception when trying to create the field westy / frogs.");
//      } catch (ClientException e) {
//      }
      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting testing DB: " + e);
    }
  }

  @Test
  public void test_180_DBUpserts() {
    HashSet<String> expected = null;
    HashSet<String> actual = null;
    try {

      client.fieldAppendOrCreate(westyEntry.getGuid(), "dogs", "bear", westyEntry);
      expected = new HashSet<>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(westyEntry.getGuid(), "dogs", westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("1) Looking for bear: " + e);
    }
    try {
      client.fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);
      expected = new HashSet<>(Arrays.asList("bear", "wags", "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("2) Looking for bear, wags, tucker: " + e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue", westyEntry);
      expected = new HashSet<>(Arrays.asList("sue"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("3) Looking for sue: " + e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "william", westyEntry);
      expected = new HashSet<String>(Arrays.asList("william"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("4) Looking for william: " + e);
    }
    try {
      client.fieldReplaceOrCreateList(westyEntry.getGuid(), "goats",
              new JSONArray(Arrays.asList("dink", "tink")), westyEntry);
      expected = new HashSet<String>(Arrays.asList("dink", "tink"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("5) Looking for dink, tink: " + e);
    }
  }

  @Test
  public void test_190_Substitute() {
    String testSubstituteGuid = "testSubstituteGUID" + RandomString.randomString(6);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      //Utils.clearTestGuids(client);
      //System.out.println("cleared old GUIDs");
      testEntry = client.guidCreate(masterGuid, testSubstituteGuid);
      System.out.println("created test guid: " + testEntry);
    } catch (Exception e) {
      fail("Exception during init: " + e);
    }
    try {
      client.fieldAppendOrCreateList(testEntry.getGuid(), field, new JSONArray(Arrays.asList("Frank", "Joe", "Sally", "Rita")), testEntry);
    } catch (Exception e) {
      fail("Exception during create: " + e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList("Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }

    try {
      client.fieldSubstitute(testEntry.getGuid(), field, "Christy", "Sally", testEntry);
    } catch (Exception e) {
      fail("Exception during substitute: " + e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList("Frank", "Joe", "Christy", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_200_SubstituteList() {
    String testSubstituteListGuid = "testSubstituteListGUID" + RandomString.randomString(6);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      //Utils.clearTestGuids(client);
      //System.out.println("cleared old GUIDs");
      testEntry = client.guidCreate(masterGuid, testSubstituteListGuid);
      System.out.println("created test guid: " + testEntry);
    } catch (Exception e) {
      fail("Exception during init: " + e);
    }
    try {
      client.fieldAppendOrCreateList(testEntry.getGuid(), field, new JSONArray(Arrays.asList("Frank", "Joe", "Sally", "Rita")), testEntry);
    } catch (Exception e) {
      fail("Exception during create: " + e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList("Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }

    try {
      client.fieldSubstitute(testEntry.getGuid(), field,
              new JSONArray(Arrays.asList("BillyBob", "Hank")),
              new JSONArray(Arrays.asList("Frank", "Joe")),
              testEntry);
    } catch (Exception e) {
      fail("Exception during substitute: " + e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList("BillyBob", "Hank", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_210_GroupCreate() {
    String mygroupName = "mygroup" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(mygroupName);
        fail(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = client.guidCreate(masterGuid, "deleteMe" + RandomString.randomString(6));
      mygroupEntry = client.guidCreate(masterGuid, mygroupName);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
  }

  @Test
  public void test_211_GroupAdd() {
    try {
      client.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(), mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), guidToDeleteEntry.getGuid(), mygroupEntry);
    } catch (Exception e) {
      fail("Exception while adding to groups: " + e);
    }
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

      expected = new HashSet<String>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(westyEntry.getGuid(), westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception while getting members and groups: " + e);
    }
  }

  @Test
  public void test_212_GroupRemoveGuid() {
    // now remove a guid and check for group updates
    try {
      client.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
    waitSettle();
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  private static GuidEntry groupAccessUserEntry = null;

  @Test
  public void test_220_GroupAndACLCreateGuids() {
    //testGroup();
    String groupAccessUserName = "groupAccessUser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(groupAccessUserName);
        fail(groupAccessUserName + " entity should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Checking for existence of group user: " + e);
    }

    try {
      groupAccessUserEntry = client.guidCreate(masterGuid, groupAccessUserName);
      // remove all fields read by all
      client.aclRemove(AclAccessType.READ_WHITELIST, groupAccessUserEntry, GNSCommandProtocol.ALL_FIELDS, GNSCommandProtocol.ALL_GUIDS);
    } catch (Exception e) {
      fail("Exception creating group user: " + e);
    }

    try {
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "address", "23 Jumper Road", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "age", "43", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "hometown", "whoville", groupAccessUserEntry);
    } catch (Exception e) {
      fail("Exception creating group user fields: " + e);
    }
    try {
      client.aclAdd(AclAccessType.READ_WHITELIST, groupAccessUserEntry, "hometown", mygroupEntry.getGuid());
    } catch (Exception e) {
      fail("Exception adding mygroup to acl for group user hometown field: " + e);
    }
  }

  @Test
  public void test_221_GroupAndACLTestBadAccess() {
    try {
      try {
        String result = client.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "address", westyEntry);
        fail("Result of read of groupAccessUser's age by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while attempting a failing read of groupAccessUser's age by sam: " + e);
    }
  }

  @Test
  public void test_222_GroupAndACLTestGoodAccess() {
    try {
      assertEquals("whoville", client.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (Exception e) {
      fail("Exception while attempting read of groupAccessUser's hometown by westy: " + e);
    }
  }

  @Test
  public void test_223_GroupAndACLTestRemoveGuid() {
    try {
      try {
        client.groupRemoveGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      } catch (Exception e) {
        fail("Exception removing westy from mygroup: " + e);
      }

      HashSet<String> expected = new HashSet<String>(Arrays.asList(samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  private static String alias = "ALIAS-" + RandomString.randomString(4) + "@blah.org";

  @Test
  public void test_230_AliasAdd() {
    try {
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      // add an alias to the masterGuid
      client.addAlias(masterGuid, alias);
      // lookup the guid using the alias
      assertEquals(masterGuid.getGuid(), client.lookupGuid(alias));
    } catch (Exception e) {
      fail("Exception while adding alias: " + e);
    }
  }

  @Test
  public void test_231_AliasRemove() {
    try {
      // grab all the alias from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.getAliases(masterGuid));
      // make sure our new one is in there
      assertThat(actual, hasItem(alias));
      // now remove it 
      client.removeAlias(masterGuid, alias);
    } catch (Exception e) {
      fail("Exception removing alias: " + e);
    }
  }

  @Test
  public void test_232_AliasCheck() {
    try {
      // an make sure it is gone
      try {
        client.lookupGuid(alias);
        fail(alias + " should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while checking alias: " + e);
    }
  }

  @Test
  public void test_240_WriteAccess() {
    String fieldName = "whereAmI";
    try {
      try {
        client.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, fieldName, samEntry.getGuid());
      } catch (Exception e) {
        fail("Exception adding Sam to Westy's writelist: " + e);
        e.printStackTrace();
      }
      // write my own field
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "shopping", westyEntry);
      } catch (Exception e) {
        fail("Exception while Westy's writing own field: " + e);
        e.printStackTrace();
      }
      // now check the value
      assertEquals("shopping", client.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldName, westyEntry));
      // someone else write my field
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "driving", samEntry);
      } catch (Exception e) {
        fail("Exception while Sam writing Westy's field: " + e);
        e.printStackTrace();
      }
      // now check the value
      assertEquals("driving", client.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldName, westyEntry));
      // do one that should fail
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(), fieldName, "driving", barneyEntry);
        fail("Write by barney should have failed!");
      } catch (ClientException e) {
      } catch (Exception e) {
        e.printStackTrace();
        fail("Exception during read of westy's " + fieldName + " by sam: " + e);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_250_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedReadFieldName, "funkadelicread", westyEntry);
      client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, unsignedReadFieldName, GNSCommandProtocol.ALL_GUIDS);
      assertEquals("funkadelicread", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedReadFieldName, null));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardReadFieldName, "bummer", westyEntry);
      // already did this above... doing it again gives us a paxos error
      //client.removeFromACL(AclAccessType.READ_WHITELIST, westyEntry, GNSCommandProtocol.ALL_FIELDS, GNSCommandProtocol.ALL_GUIDS);
      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), standardReadFieldName, null);
        fail("Result of read of westy's " + standardReadFieldName + " as world readable was " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_260_UnsignedWrite() {
    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      client.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry, unsignedWriteFieldName, GNSCommandProtocol.ALL_GUIDS);
      client.fieldReplaceFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      assertEquals("funkadelicwrite", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardWriteFieldName, "bummer", westyEntry);
      try {
        client.fieldReplaceFirstElementTest(westyEntry.getGuid(), standardWriteFieldName, "funkadelicwrite", null);
        fail("Write of westy's field " + standardWriteFieldName + " as world readable should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_270_RemoveField() {
    String fieldToDelete = "fieldToDelete";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), fieldToDelete, "work", westyEntry);
    } catch (IOException | ClientException e) {
      fail("Exception while creating the field: " + e);
    }
    try {
      // read my own field
      assertEquals("work", client.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldToDelete, westyEntry));
    } catch (Exception e) {
      fail("Exception while reading the field " + fieldToDelete + ": " + e);
    }
    try {
      client.fieldRemove(westyEntry.getGuid(), fieldToDelete, westyEntry);
    } catch (IOException | InvalidKeyException | NoSuchAlgorithmException | SignatureException | ClientException e) {
      fail("Exception while removing field: " + e);
    }

    try {
      String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldToDelete, westyEntry);

      fail("Result of read of westy's " + fieldToDelete + " is " + result
              + " which is wrong because it should have been deleted.");
    } catch (ClientException e) {
    } catch (Exception e) {
      fail("Exception while removing field: " + e);
    }

  }

  @Test
  public void test_280_ListOrderAndSetElement() {
    try {
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
    } catch (Exception e) {
      fail("Exception during creation of westyEntry: " + e);
    }
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "numbers", "one", westyEntry);

      assertEquals("one", client.fieldReadArrayFirstElement(westyEntry.getGuid(), "numbers", westyEntry));

      client.fieldAppend(westyEntry.getGuid(), "numbers", "two", westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "three", westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "four", westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "five", westyEntry);

      List<String> expected = new ArrayList<>(Arrays.asList("one", "two", "three", "four", "five"));
      ArrayList<String> actual = JSONUtils.JSONArrayToArrayList(client.fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      assertEquals(expected, actual);

      client.fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2, westyEntry);

      expected = new ArrayList<>(Arrays.asList("one", "two", "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(client.fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Unexpected exception during test: " + e);
    }
  }

  @Test
  public void test_310_BasicSelect() {
    try {
      JSONArray result = client.select("cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }
  
  @Test
  public void test_320_GeoSpatialSelect() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "geoTest-" + RandomString.randomString(6));
        client.setLocation(testEntry, 0.0, 0.0);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = client.selectNear(GNSCommandProtocol.LOCATION_FIELD_NAME, loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectNear: " + e);
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
      JSONArray result = client.selectWithin(GNSCommandProtocol.LOCATION_FIELD_NAME, rect);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: " + e);
    }
  }

  @Test
  public void test_330_QuerySelect() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry);
      }
    } catch (Exception e) {
      fail("Exception while tryint to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectNear: " + e);
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
      JSONArray result = client.selectWithin(GNSCommandProtocol.LOCATION_FIELD_NAME, rect);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: " + e);
    }
  }

  @Test
  public void test_400_SetFieldNull() {
    String field = "fieldToSetToNull";
    try {
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), field, "work", westyEntry);
    } catch (Exception e) {
      fail("Exception while creating the field: " + e);
    }
    try {
      // read my own field
      assertEquals("work", client.fieldReadArrayFirstElement(westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      fail("Exception while reading the field " + field + ": " + e);
    }
    try {
      client.fieldSetNull(westyEntry.getGuid(), field, westyEntry);
    } catch (Exception e) {
      fail("Exception while setting field to null field: " + e);
    }

    try {
      assertEquals(null, client.fieldReadArrayFirstElement(westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      fail("Exception while reading the field " + field + ": " + e);
    }
  }

  @Test
  public void test_410_JSONUpdate() {
    try {
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
    try {
      JSONObject json = new JSONObject();
      json.put("name", "frank");
      json.put("occupation", "busboy");
      json.put("location", "work");
      json.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

  @Test
  public void test_420_NewRead() {
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
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
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally\": " + e);
    }

    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack", westyEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack\": " + e);
    }
  }

  @Test
  public void test_430_NewUpdate() {
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sammy", new ArrayList<String>(Arrays.asList("One", "Ready", "Frap")), westyEntry);
    } catch (Exception e) {
      fail("Exception while updating field \"flapjack.sammy\": " + e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", new ArrayList<String>(Arrays.asList("One", "Ready", "Frap")));
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash", new ArrayList<String>(Arrays.asList("Tango", "Sierra", "Alpha")));
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
      expected.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash", new ArrayList<String>(Arrays.asList("Tango", "Sierra", "Alpha")));
      expected.put("flapjack", moreJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

  private static final String BYTE_TEST_FIELD = "testBytes";
  private static byte[] byteTestValue;

  @Test
  public void test_440_CreateBytesField() {
    try {
      byteTestValue = RandomUtils.nextBytes(16000);
      String encodedValue = Base64.encodeToString(byteTestValue, true);
      //System.out.println("Encoded string: " + encodedValue);
      client.fieldUpdate(masterGuid, BYTE_TEST_FIELD, encodedValue);
    } catch (IOException | ClientException | JSONException e) {
      fail("Exception during create field: " + e);
    }
  }

  @Test
  public void test_441_ReadBytesField() {
    try {
      String string = client.fieldRead(masterGuid, BYTE_TEST_FIELD);
      //System.out.println("Read string: " + string);
      assertArrayEquals(byteTestValue, Base64.decode(string));
    } catch (Exception e) {
      fail("Exception while reading field: " + e);
    }
  }

  private static int numberTocreate = 100;
  private static GuidEntry accountGuidForBatch = null;

  @Test
  public void test_510_CreateBatchAccountGuid() {
    // can change the number to create on the command line
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      accountGuidForBatch = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_511_CreateBatch() {
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      aliases.add("testGUID" + RandomString.randomString(6));
    }
    String result = null;
    long oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(15 * 1000); // 30 seconds
      result = client.guidBatchCreate(accountGuidForBatch, aliases);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GNSCommandProtocol.OK_RESPONSE, result);
  }

  @Test
  public void test_512_CheckBatch() {
    try {
      JSONObject accountRecord = client.lookupAccountRecord(accountGuidForBatch.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

//  private static GuidEntry accountGuidForWithoutPublicKeys = null;
//
//  @Test
//  public void test_520_CreateBatchAccountGuidForWithoutPublicKeys() {
//    try {
//      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
//      accountGuidForWithoutPublicKeys = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it: " + e);
//    }
//  }
//
//  @Test
//  public void test_521_CreateBatchWithoutPublicKeys() {
//    Set<String> aliases = new HashSet<>();
//    for (int i = 0; i < numberTocreate; i++) {
//      aliases.add("testGUID" + RandomString.randomString(6));
//    }
//    String result = null;
//    int oldTimeout = client.getReadTimeout();
//    try {
//      client.setReadTimeout(15 * 1000); // 30 seconds
//      result = client.guidBatchCreate(accountGuidForWithoutPublicKeys, aliases, false);
//      client.setReadTimeout(oldTimeout);
//    } catch (Exception e) {
//      fail("Exception while creating guids: " + e);
//    }
//    assertEquals(GNSCommandProtocol.OK_RESPONSE, result);
//  }
//
//  @Test
//  public void test_522_CheckBatchForWithoutPublicKeys() {
//    try {
//      JSONObject accountRecord = client.lookupAccountRecord(accountGuidForWithoutPublicKeys.getGuid());
//      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
//    } catch (JSONException | ClientException | IOException e) {
//      fail("Exception while fetching account record: " + e);
//    }
//  }
//
//  private static GuidEntry accountGuidForFastest = null;
//
//  @Test
//  public void test_530_CreateBatchAccountGuidForForFastest() {
//    try {
//      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
//      accountGuidForFastest = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it: " + e);
//    }
//  }

//  @Test
//  public void test_531_CreateBatchFastest() {
//    String result = null;
//    int oldTimeout = client.getReadTimeout();
//    try {
//      client.setReadTimeout(15 * 1000); // 30 seconds
//      result = client.guidBatchCreateFast(accountGuidForFastest, numberTocreate);
//      client.setReadTimeout(oldTimeout);
//    } catch (Exception e) {
//      fail("Exception while creating guids: " + e);
//    }
//    assertEquals(GNSCommandProtocol.OK_RESPONSE, result);
//  }
//
//  @Test
//  public void test_532_CheckBatchForFastest() {
//    try {
//      JSONObject accountRecord = client.lookupAccountRecord(accountGuidForFastest.getGuid());
//      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
//    } catch (JSONException | ClientException | IOException e) {
//      fail("Exception while fetching account record: " + e);
//    }
//  }

  private static String createIndexTestField;

  @Test
  public void test_810_CreateField() {
    createIndexTestField = "testField" + RandomString.randomString(6);
    try {
      client.fieldUpdate(masterGuid, createIndexTestField, createGeoJSONPolygon(AREA_EXTENT));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during create field: " + e);
    }
  }

  @Test
  public void test_820_CreateIndex() {
    try {
      client.fieldCreateIndex(masterGuid, createIndexTestField, "2dsphere");
    } catch (Exception e) {
      fail("Exception while creating index: " + e);
    }
  }

  @Test
  public void test_830_SelectPass() {
    try {
      JSONArray result = client.selectQuery(buildQuery(createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (Exception e) {
      fail("Exception executing second selectNear: " + e);
    }
  }

  // HELPER STUFF
  private static final String POLYGON = "Polygon";
  private static final String COORDINATES = "coordinates";
  private static final String TYPE = "type";

  private static JSONObject createGeoJSONPolygon(List<Point2D> coordinates) throws JSONException {
    JSONArray ring = new JSONArray();
    for (int i = 0; i < coordinates.size(); i++) {
      ring.put(i, new JSONArray(
              Arrays.asList(coordinates.get(i).getX(), coordinates.get(i).getY())));
    }
    JSONArray jsonCoordinates = new JSONArray();
    jsonCoordinates.put(0, ring);
    JSONObject json = new JSONObject();
    json.put(TYPE, POLYGON);
    json.put(COORDINATES, jsonCoordinates);
    return json;
  }

  private static final double LEFT = -98.08;
  private static final double RIGHT = -96.01;
  private static final double TOP = 33.635;
  private static final double BOTTOM = 31.854;

  private static final Point2D UPPER_LEFT = new Point2D.Double(LEFT, TOP);
  //private static final GlobalCoordinate UPPER_LEFT = new GlobalCoordinate(33.45, -98.08);
  private static final Point2D UPPER_RIGHT = new Point2D.Double(RIGHT, TOP);
  //private static final GlobalCoordinate UPPER_RIGHT = new GlobalCoordinate(33.45, -96.01);
  private static final Point2D LOWER_RIGHT = new Point2D.Double(RIGHT, BOTTOM);
  //private static final GlobalCoordinate LOWER_RIGHT = new GlobalCoordinate(32.23, -96.01);
  private static final Point2D LOWER_LEFT = new Point2D.Double(LEFT, BOTTOM);
  //private static final GlobalCoordinate LOWER_LEFT = new GlobalCoordinate(32.23, -98.08);

  private static final List<Point2D> AREA_EXTENT = new ArrayList<>(
          Arrays.asList(UPPER_LEFT, UPPER_RIGHT, LOWER_RIGHT, LOWER_LEFT, UPPER_LEFT));

  private static String buildQuery(String locationField, List<Point2D> coordinates) throws JSONException {
    return "~" + locationField + ":{"
            + "$geoIntersects :{"
            + "$geometry:"
            + createGeoJSONPolygon(coordinates).toString()
            + "}"
            + "}";
  }

  @Test
  public void test_999_Stop() {
    try {
      client.close();
    } catch (Exception e) {
      fail("Exception during stop: " + e);
    }
  }
}
