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

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.GnsProtocol.AccessType;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.exceptions.client.GnsFieldNotFoundException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.hamcrest.Matchers.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Functionality test for core elements in the client using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UniversalCoreUpToGroup {

  private static String accountAlias = "test@cgns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static String password = "password";
  private static UniversalTcpClientExtended client = null;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;

  public UniversalCoreUpToGroup() {
    if (client == null) {
      if (System.getProperty("host") != null
              && !System.getProperty("host").isEmpty()
              && System.getProperty("port") != null
              && !System.getProperty("port").isEmpty()) {
        address = new InetSocketAddress(System.getProperty("host"),
                Integer.parseInt(System.getProperty("port")));
      } else {
        address = ServerSelectDialog.selectServer();
      }
      System.out.println("Connecting to " + address.getHostName() + ":" + address.getPort());
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(),
              System.getProperty("disableSSL").equals("true"));
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
    String alias = "testGUID" + RandomString.randomString(6);
    GuidEntry guidEntry = null;
    try {
      guidEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, alias);
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
    assertNotNull(guidEntry);
    assertEquals(alias, guidEntry.getEntityName());
  }

  @Test
  public void test_020_RemoveGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, testGuidName);
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
    } catch (GnsClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  @Test
  public void test_030_RemoveGuidSansAccountInfo() {
    String testGuidName = "testGUID" + RandomString.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, testGuidName);
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
    } catch (GnsClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  @Test
  public void test_040_LookupPrimaryGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(6);
    GuidEntry testGuid = null;
    try {
      testGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: " + e);
    }
    try {
      assertEquals(masterGuid.getGuid(), client.lookupPrimaryGuid(testGuid.getGuid()));
    } catch (Exception e) {
      fail("Exception while looking up primary guid for testGuid: " + e);
    }
  }

  @Test
  public void test_050_CreateSubGuid() {
    try {
      subGuidEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "subGuid" + RandomString.randomString(6));
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
    } catch (GnsFieldNotFoundException e) {
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
    } catch (Exception e) {
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
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      samEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception registering guids for create fields: " + e);
    }
    try {
      // remove default read acces for this test
      client.aclRemove(AccessType.READ_WHITELIST, westyEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
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
      } catch (GnsClientException e) {
      }
    } catch (Exception e) {
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
        client.aclAdd(AccessType.READ_WHITELIST, westyEntry, "environment",
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
      } catch (GnsClientException e) {
      } catch (Exception e) {
        fail("Exception looking up Barney: " + e);
        e.printStackTrace();
      }
      barneyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, barneyName);
      // remove default read access for this test
      client.aclRemove(AccessType.READ_WHITELIST, barneyEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);

      try {
        // let anybody read barney's cell field
        client.aclAdd(AccessType.READ_WHITELIST, barneyEntry, "cell",
                GnsProtocol.ALL_USERS);
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
      } catch (GnsClientException e) {
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
      } catch (GnsClientException e) {
      }

      GuidEntry superuserEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AccessType.READ_WHITELIST, barneyEntry, GnsProtocol.ALL_FIELDS, superuserEntry.getGuid());

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
        client.aclAdd(AccessType.READ_WHITELIST, westyEntry, "test.deeper.field", GnsProtocol.ALL_FIELDS);
      } catch (Exception e) {
        fail("Problem adding acl: " + e);
      }
      try {
        JSONArray actual = client.aclGet(AccessType.READ_WHITELIST, westyEntry,
                "test.deeper.field", westyEntry.getGuid());
        JSONArray expected = new JSONArray(new ArrayList(Arrays.asList(GnsProtocol.ALL_FIELDS)));
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

      HashSet<String> expected = new HashSet<String>(Arrays.asList("hooch",
              "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      expected = new HashSet<String>(Arrays.asList("hooch", "red", "sox",
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
//      } catch (GnsClientException e) {
//      }
      //this one always fails... check it out
//      try {
//        client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "frogs", "freddybub",
//                westyEntry);
//        fail("Should have got an exception when trying to create the field westy / frogs.");
//      } catch (GnsClientException e) {
//      }
      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<String>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<String>(Arrays.asList("maya", "fred"));
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
      expected = new HashSet<String>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(westyEntry.getGuid(), "dogs", westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("1) Looking for bear: " + e);
    }
    try {
      client.fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);
      expected = new HashSet<String>(Arrays.asList("bear", "wags", "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("2) Looking for bear, wags, tucker: " + e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue", westyEntry);
      expected = new HashSet<String>(Arrays.asList("sue"));
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
      testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, testSubstituteGuid);
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
      testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, testSubstituteListGuid);
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
      } catch (GnsClientException e) {
      }
      guidToDeleteEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "deleteMe" + RandomString.randomString(6));
      mygroupEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, mygroupName);
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
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (GnsClientException e) {

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
      } catch (GnsClientException e) {
      }
    } catch (Exception e) {
      fail("Checking for existence of group user: " + e);
    }

    try {
      groupAccessUserEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupAccessUserName);
      // remove all fields read by all
      client.aclRemove(AccessType.READ_WHITELIST, groupAccessUserEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
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
      client.aclAdd(AccessType.READ_WHITELIST, groupAccessUserEntry, "hometown", mygroupEntry.getGuid());
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
      } catch (GnsClientException e) {
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
  public void test_999_Stop() {
    try {
      client.stop();
    } catch (Exception e) {
      fail("Exception during stop: " + e);
    }
  }
}
