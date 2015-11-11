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
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnsclient.exceptions.GnsFieldNotFoundException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import static org.hamcrest.Matchers.*;
import org.json.JSONArray;
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
public class UniversalTcpClientWithSSLTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
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

  public UniversalTcpClientWithSSLTest() {
    if (client == null) {
      address = ServerSelectDialog.selectServer();
      System.out.println("Connecting to " + address.getHostName() + ":" + address.getPort());
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort());
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void test_01_CreateEntity() {
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
  public void test_02_RemoveGuid() {
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
    int cnt = 0;
    try {
      do {
        try {
          client.lookupGuidRecord(testGuid.getGuid());
          if (cnt++ > 10) {
            fail(testGuid.getGuid() + " should not exist (after 10 checks)");
            break;
          }
        } catch (IOException e) {
          fail("Exception while looking up alias: " + e);
        }
        ThreadUtils.sleep(10);
      } while (true);
      // the lookup should fail and throw to here
    } catch (GnsException e) {
    }
//    try {
//      client.lookupGuidRecord(testGuid.getGuid());
//      fail("Lookup testGuid should have throw an exception.");
//    } catch (GnsException e) {
//
//    } catch (IOException e) {
//      fail("Exception while doing Lookup testGuid: " + e);
//    }
  }

  @Test
  public void test_03_RemoveGuidSansAccountInfo() {
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
    } catch (GnsException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  @Test
  public void test_04_LookupPrimaryGuid() {
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
  public void test_05_CreateSubGuid() {
    try {
      subGuidEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "subGuid" + RandomString.randomString(6));
      System.out.println("Created: " + subGuidEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_06_FieldNotFoundException() {
    try {
      client.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry);
      fail("Should have thrown an exception.");
    } catch (GnsFieldNotFoundException e) {
      System.out.println("This was expected: " + e);
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_07_FieldExistsFalse() {
    try {
      assertFalse(client.fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_08_CreateFieldForFieldExists() {
    try {
      client.fieldCreateOneElementList(subGuidEntry.getGuid(), "environment", "work", subGuidEntry);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during create field: " + e);
    }
  }

  @Test
  public void test_09_FieldExistsTrue() {
    try {
      assertTrue(client.fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_10_CreateFields() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      samEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
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
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_11_ACLPartOne() {
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
      fail("Exception when we were not expecting it: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_12_ACLPartTwo() {
    try {
      String barneyName = "barney" + RandomString.randomString(6);
      try {
        client.lookupGuid(barneyName);
        fail(barneyName + " entity should not exist");
      } catch (GnsException e) {
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
      } catch (GnsException e) {
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' address: " + e);
        e.printStackTrace();
      }

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_13_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        fail(superUserName + " entity should not exist");
      } catch (GnsException e) {
      }

      GuidEntry superuserEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AccessType.READ_WHITELIST, barneyEntry, GnsProtocol.ALL_FIELDS, superuserEntry.getGuid());

      assertEquals("413-555-1234",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_14_DB() {
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
//      } catch (GnsException e) {
//      }
      //this one always fails... check it out
//      try {
//        client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "frogs", "freddybub",
//                westyEntry);
//        fail("Should have got an exception when trying to create the field westy / frogs.");
//      } catch (GnsException e) {
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
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_15_DBUpserts() {
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
  public void test_16_Substitute() {
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
  public void test_17_SubstituteList() {
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
  public void test_18_Group() {
    String mygroupName = "mygroup" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(mygroupName);
        fail(mygroupName + " entity should not exist");
      } catch (GnsException e) {
      }
      guidToDeleteEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "deleteMe" + RandomString.randomString(6));
      mygroupEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, mygroupName);

      client.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(), mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), guidToDeleteEntry.getGuid(), mygroupEntry);

      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

      expected = new HashSet<String>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(westyEntry.getGuid(), westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
      System.exit(2);
    }
    // now remove a guid and check for group updates
    try {
      client.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (GnsException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception during remove guid group update test: " + e);
      System.exit(2);
    }

  }

  @Test
  public void test_19_GroupAndACL() {
    //testGroup();
    String groupAccessUserName = "groupAccessUser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(groupAccessUserName);
        fail(groupAccessUserName + " entity should not exist");
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Checking for existence of group user: " + e);
    }
    GuidEntry groupAccessUserEntry = null;

    try {
      groupAccessUserEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupAccessUserName);
      // remove all fields read by all
      client.aclRemove(AccessType.READ_WHITELIST, groupAccessUserEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
    } catch (Exception e) {
      fail("Exception creating group user: " + e);
      return;
    }

    try {
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "address", "23 Jumper Road", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "age", "43", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "hometown", "whoville", groupAccessUserEntry);
    } catch (Exception e) {
      fail("Exception creating group user fields: " + e);
      return;
    }
    try {
      client.aclAdd(AccessType.READ_WHITELIST, groupAccessUserEntry, "hometown", mygroupEntry.getGuid());
    } catch (Exception e) {
      fail("Exception adding mygroup to acl for group user hometown field: " + e);
      return;
    }
    try {
      try {
        String result = client.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "address", westyEntry);
        fail("Result of read of groupAccessUser's age by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Exception while attempting a failing read of groupAccessUser's age by sam: " + e);
    }
    try {
      assertEquals("whoville", client.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (Exception e) {
      fail("Exception while attempting read of groupAccessUser's hometown by westy: " + e);
    }
    try {
      try {
        client.groupRemoveGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      } catch (Exception e) {
        fail("Exception removing westy from mygroup: " + e);
        return;
      }

      HashSet<String> expected = new HashSet<String>(Arrays.asList(samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  private static final String alias = "ALIAS-" + RandomString.randomString(4) + "@blah.org";

  @Test
  public void test_20_AliasAdd() {
    try {
      //
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      //
      // add an alias to the masterGuid
      client.addAlias(masterGuid, alias);
      // lookup the guid using the alias
      assertEquals(masterGuid.getGuid(), client.lookupGuid(alias));

      // grab all the aliases from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.getAliases(masterGuid));
      // make sure our new one is in there
      assertThat(actual, hasItem(alias));

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_21_AliasRemove() {
    try {
      // now remove it 
      client.removeAlias(masterGuid, alias);
    } catch (Exception e) {
      fail("Exception while removing alias: " + e);
    }
    try {
      client.lookupGuid(alias);
      System.out.println(alias + " should not exist");
    } catch (GnsException e) {
    } catch (IOException e) {
      fail("Exception while looking up alias: " + e);
    }
    // alternative test when the remove didn't work immediately
    // make sure it is gone
//    int cnt = 0;
//    try {
//      do {
//        try {
//          client.lookupGuid(alias);
//          if (cnt++ > 10) {
//            fail(alias + " should not exist (after 10 checks)");
//            break;
//          }
//
//        } catch (IOException e) {
//          fail("Exception while looking up alias: " + e);
//        }
//        ThreadUtils.sleep(10);
//      } while (true);
//      // the lookup should fail and throw to here
//    } catch (GnsException e) {
//    }
  }

  @Test
  public void test_31_BasicSelect() {
    try {
      JSONArray result = client.select("cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_32_GeoSpatialSelect() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "geoTest-" + RandomString.randomString(6));
        client.setLocation(testEntry, 0.0, 0.0);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = client.selectNear(GnsProtocol.LOCATION_FIELD_NAME, loc, 2000000.0);
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
      JSONArray result = client.selectWithin(GnsProtocol.LOCATION_FIELD_NAME, rect);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: " + e);
    }
  }

  @Test
  public void test_33_QuerySelect() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
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
      JSONArray result = client.selectWithin(GnsProtocol.LOCATION_FIELD_NAME, rect);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: " + e);
    }
  }

  @Test
  public void test_44_WriteAccess() {
    String fieldName = "whereAmI";
    try {
      try {
        client.aclAdd(AccessType.WRITE_WHITELIST, westyEntry, fieldName, samEntry.getGuid());
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
      } catch (GnsException e) {
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
  public void test_45_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedReadFieldName, "funkadelicread", westyEntry);
      client.aclAdd(AccessType.READ_WHITELIST, westyEntry, unsignedReadFieldName, GnsProtocol.ALL_USERS);
      assertEquals("funkadelicread", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedReadFieldName, null));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardReadFieldName, "bummer", westyEntry);
      // already did this above... doing it again gives us a paxos error
      //client.removeFromACL(AccessType.READ_WHITELIST, westyEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), standardReadFieldName, null);
        fail("Result of read of westy's " + standardReadFieldName + " as world readable was " + result
                + " which is wrong because it should have been rejected.");
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_46_UnsignedWrite() {
    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      client.aclAdd(AccessType.WRITE_WHITELIST, westyEntry, unsignedWriteFieldName, GnsProtocol.ALL_USERS);
      client.fieldReplaceFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      assertEquals("funkadelicwrite", client.fieldReadArrayFirstElement(westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      client.fieldCreateOneElementList(westyEntry.getGuid(), standardWriteFieldName, "bummer", westyEntry);
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(), standardWriteFieldName, "funkadelicwrite", null);
        fail("Write of westy's field " + standardWriteFieldName + " as world readable should have been rejected.");
      } catch (GnsException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_47_RemoveField() {
    String fieldToDelete = "fieldToDelete";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), fieldToDelete, "work", westyEntry);
    } catch (Exception e) {
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
    } catch (Exception e) {
      fail("Exception while removing field: " + e);
    }

    try {
      String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), fieldToDelete, westyEntry);
      fail("Result of read of westy's " + fieldToDelete + " is " + result
              + " which is wrong because it should have been deleted.");
    } catch (GnsException e) {
    } catch (Exception e) {
      fail("Exception while removing field: " + e);
    }

  }

  @Test
  public void test_48_ListOrderAndSetElement() {
//    try {
//      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
//    } catch (Exception e) {
//      fail("Exception during creation of westyEntry: " + e);
//    }
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "numbers", "one", westyEntry);

      assertEquals("one", client.fieldReadArrayFirstElement(westyEntry.getGuid(), "numbers", westyEntry));

      client.fieldAppend(westyEntry.getGuid(), "numbers", "two", westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "three", westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "four", westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "five", westyEntry);

      List<String> expected = new ArrayList<String>(Arrays.asList("one", "two", "three", "four", "five"));
      ArrayList<String> actual = JSONUtils.JSONArrayToArrayList(client.fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      assertEquals(expected, actual);

      client.fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2, westyEntry);

      expected = new ArrayList<String>(Arrays.asList("one", "two", "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(client.fieldReadArray(westyEntry.getGuid(), "numbers", westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Unexpected exception during test: " + e);
    }
  }

  private static final String groupTestFieldName = "_SelectAutoGroupTestQueryField_";
  private static GuidEntry groupOneGuid;
  private static GuidEntry groupTwoGuid;
  private static String queryOne = "~" + groupTestFieldName + " : {$gt: 20}";
  private static String queryTwo = "~" + groupTestFieldName + " : 0";

  @Test
  public void test_50_QueryRemovePreviousTestFields() {
    // find all the guids that have our field and remove it from them
    try {
      String query = "~" + groupTestFieldName + " : {$exists: true}";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
        System.out.println("Removing from " + guidEntry.getEntityName());
        client.fieldRemove(guidEntry, groupTestFieldName);
      }
    } catch (Exception e) {
      fail("Trying to remove previous test's fields: " + e);
    }
  }

  @Test
  public void test_51_QuerySetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(10));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupOneGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryOne.getBytes()), false);
      groupOneGuid = GuidUtils.lookupOrCreateGuidEntry(groupOneGuidName, client.getGnsRemoteHost(), client.getGnsRemotePort());
      //groupGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupGuidName + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupTwoGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryTwo.getBytes()), false);
      groupTwoGuid = GuidUtils.lookupOrCreateGuidEntry(groupTwoGuidName, client.getGnsRemoteHost(), client.getGnsRemotePort());
      //groupTwoGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupTwoGuidName + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
  }

  @Test
  public void test_52_QuerySetupGroup() {
    try {
      String query = "~" + groupTestFieldName + " : {$gt: 20}";
      JSONArray result = client.selectSetupGroupQuery(masterGuid, groupOneGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP guid named " + groupOneGuid.getEntityName() + ": ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectSetupGroupQuery: " + e);
    }
  }

  // make a second group that is empty
  @Test
  public void test_53_QuerySetupSecondGroup() {
    try {
      String query = "~" + groupTestFieldName + " : 0";
      JSONArray result = client.selectSetupGroupQuery(masterGuid, groupTwoGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP SECOND guid named " + groupTwoGuid.getEntityName() + ": (should be empty) ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // should be nothing in this group now
      assertThat(result.length(), equalTo(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing second selectSetupGroupQuery: " + e);
    }
  }

  @Test
  public void test_54_QueryLookupGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  public void test_55_QueryLookupGroupAgain() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  public void test_56_QueryLookupGroupAgain2() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  public void test_57_QueryLookupGroupAgain3() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  private void checkTheReturnValues(JSONArray result) throws Exception {
    // should be 5
    assertThat(result.length(), equalTo(5));
    // look up the individual values
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
      String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
      assertEquals("25", value);
    }
  }

  @Test
  // Change all the testQuery fields except 1 to be equal to zero
  public void test_58_QueryAlterGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      // change ALL BUT ONE to be ZERO
      for (int i = 0; i < result.length() - 1; i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
        JSONArray array = new JSONArray(Arrays.asList(0));
        client.fieldReplaceOrCreateList(entry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to alter the fields: " + e);
    }
  }

  @Test
  public void test_59_QueryLookupGroupAfterAlterations() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      // should only be one
      assertThat(result.length(), equalTo(1));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
        String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
        assertEquals("25", value);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  // Check to see if the second group has members now... it should.
  public void test_60_QueryLookupSecondGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupTwoGuid.getGuid());
      // should be 4 now
      assertThat(result.length(), equalTo(4));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
        String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
        assertEquals("0", value);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  public void test_70_SetFieldNull() {
    String field = "fieldToSetToNull";
//    try {
//      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
//      System.out.println("Created: " + westyEntry);
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it: " + e);
//    }
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

  private static GuidEntry updateEntry;

  @Test
  public void test_71_JSONUpdate() {
    try {
      updateEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      System.out.println("Created: " + updateEntry);
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
      client.update(updateEntry, json);
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
      JSONObject actual = client.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      client.update(updateEntry, json);
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
      JSONObject actual = client.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading change of \"occupation\" to \"rocket scientist\": " + e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      client.update(updateEntry, json);
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
      JSONObject actual = client.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }

    try {
      client.fieldRemove(updateEntry.getGuid(), "gibberish", updateEntry);
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
      JSONObject actual = client.read(updateEntry);
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
      client.update(updateEntry, json);
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
      JSONObject actual = client.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      String actual = client.fieldRead(updateEntry.getGuid(), "flapjack.sally.right", updateEntry);
      assertEquals("seven", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally.right\": " + e);
    }
    try {
      String actual = client.fieldRead(updateEntry.getGuid(), "flapjack.sally", updateEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //String expected = "{\"left\":\"eight\",\"right\":\"seven\"}";
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally\": " + e);
    }
    try {
      String actual = client.fieldRead(updateEntry.getGuid(), "flapjack", updateEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      //String expected = "{\"sammy\":\"green\",\"sally\":{\"left\":\"eight\",\"right\":\"seven\"}}";
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack\": " + e);
    }
  }

  @Test
  public void test_73_NewUpdate() {
    try {
      client.fieldUpdate(updateEntry.getGuid(), "flapjack.sally.right", "crank", updateEntry);
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
      JSONObject actual = client.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
    try {
      client.fieldUpdate(updateEntry.getGuid(), "flapjack.sammy", new ArrayList(Arrays.asList("One", "Ready", "Frap")), updateEntry);
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
      JSONObject actual = client.read(updateEntry);
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
      client.fieldUpdate(updateEntry.getGuid(), "flapjack", moreJson, updateEntry);
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
      JSONObject actual = client.read(updateEntry);
      JSONAssert.assertEquals(expected, actual, true);
      //System.out.println(actual);
    } catch (Exception e) {
      fail("Exception while reading JSON: " + e);
    }
  }

  @Test
  public void test_74_MultiFieldLookup() {
    try {
      String actual = client.fieldRead(updateEntry, new ArrayList(Arrays.asList("name", "occupation")));
      JSONAssert.assertEquals("{\"name\":\"frank\",\"occupation\":\"rocket scientist\"}", actual, true);
    } catch (Exception e) {
      fail("Exception while reading \"name\" and \"occupation\": " + e);
    }
  }

  private static final String indirectionGroupTestFieldName = "_IndirectionTestQueryField_";
  private static GuidEntry indirectionGroupGuid;
  private static JSONArray indirectionGroupMembers = new JSONArray();

  @Test
  public void test_75_IndirectionSetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
        indirectionGroupMembers.put(testEntry.getGuid());
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry, indirectionGroupTestFieldName, array);
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      indirectionGroupGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTestGroup-" + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      client.groupAddGuids(indirectionGroupGuid.getGuid(), indirectionGroupMembers, masterGuid);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  @Test
  public void test_76_IndirectionTestRead() {
    try {
      String actual = client.fieldRead(indirectionGroupGuid, indirectionGroupTestFieldName);
      System.out.println("Indirection Test Result = " + actual);
      String expected = new JSONArray(Arrays.asList(Arrays.asList(25), Arrays.asList(25), Arrays.asList(25), Arrays.asList(25), Arrays.asList(25))).toString();
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to read the " + indirectionGroupTestFieldName + " of the group guid: " + e);
    }
  }

  @Test
  public void test_77_Stop() {
    try {
      client.stop();
    } catch (Exception e) {
      fail("Exception during stop: " + e);
    }
  }

}
