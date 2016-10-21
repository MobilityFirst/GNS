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
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;

import java.io.IOException;
import java.util.Arrays;

import org.json.JSONArray;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * This test
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AclDefaultsTest {

  private static final String ACCOUNT_ALIAS = "acltest@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public AclDefaultsTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_100_CreateGuid() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
  }
  
  private static final String TEST_FIELD_NAME = "testField";

  /**
   *
   */
  @Test
  public void test_110_ACLCreateField() {
    try {
      client.fieldCreateOneElementList(masterGuid.getGuid(), TEST_FIELD_NAME, "testValue", masterGuid);
    } catch (Exception e) {
      fail("Exception while creating fields in ACLCreateFields: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_120_CreateAcl() {
    try {
      client.aclCreateField(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME);
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
  }
  
  @Test
  public void test_130_CheckAcl() {
    try {
      assertTrue(client.aclFieldExists(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME));
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
  }
  
  @Test
  public void test_140_DeleteAcl() {
    try {
      client.aclDeleteField(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME);
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
  }
  
  @Test
  public void test_150_CheckAcl() {
    try {
      assertFalse(client.aclFieldExists(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME));
    } catch (Exception e) {
      fail("Exception while creating guid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_190_ACLCheckForAllFields() {
    try {
      JSONArray expected = new JSONArray(Arrays.asList(GNSCommandProtocol.ALL_GUIDS));
      JSONAssert.assertEquals(expected,
              client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSCommandProtocol.ENTIRE_RECORD, masterGuid.getGuid()), true);
    } catch (Exception e) {
      fail("Exception while checking ALL_FIELDS in ACLCheckForAllFields: " + e);
      e.printStackTrace();
    }
//    try {
//      // remove default read access for this test
//      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, 
//              GNSCommandProtocol.ENTIRE_RECORD, GNSCommandProtocol.ALL_GUIDS);
//    } catch (Exception e) {
//      fail("Exception while removing ACL in ACLCreateGuids: " + e);
//      e.printStackTrace();
//    }
//    try {
//      JSONArray expected = new JSONArray(new ArrayList<String>(Arrays.asList(masterGuid.getGuid())));
//      JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
//              GNSCommandProtocol.ENTIRE_RECORD, westyEntry.getGuid());
//      JSONAssert.assertEquals(expected, actual, true);
//    } catch (Exception e) {
//      fail("Exception while retrieving ACL in ACLCreateGuids: " + e);
//      e.printStackTrace();
//    }
  }

//  /**
//   *
//   */
//  @Test
//  public void test_101_ACLCreateFields() {
//    try {
//      client.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
//      client.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
//      client.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
//      client.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
//    } catch (Exception e) {
//      fail("Exception while creating fields in ACLCreateFields: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_102_ACLReadAllFields() {
//    try {
//      JSONObject expected = new JSONObject();
//      expected.put("environment", new JSONArray(new ArrayList<String>(Arrays.asList("work"))));
//      expected.put("password", new JSONArray(new ArrayList<String>(Arrays.asList("666flapJack"))));
//      expected.put("ssn", new JSONArray(new ArrayList<String>(Arrays.asList("000-00-0000"))));
//      expected.put("address", new JSONArray(new ArrayList<String>(Arrays.asList("100 Hinkledinkle Drive"))));
//      JSONObject actual = new JSONObject(client.fieldRead(westyEntry.getGuid(), GNSCommandProtocol.ENTIRE_RECORD, masterGuid));
//      JSONAssert.assertEquals(expected, actual, true);
//    } catch (Exception e) {
//      fail("Exception while reading all fields in ACLReadAllFields: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_104_ACLReadMyFields() {
//    try {
//      // read my own field
//      assertEquals("work",
//              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
//      // read another one of my fields field
//      assertEquals("000-00-0000",
//              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));
//
//    } catch (Exception e) {
//      fail("Exception while reading fields in ACLReadMyFields: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_105_ACLNotReadOtherGuidAllFieldsTest() {
//    try {
//      try {
//        String result = client.fieldRead(westyEntry.getGuid(), GNSCommandProtocol.ENTIRE_RECORD, samEntry);
//        fail("Result of read of all of westy's fields by sam is " + result
//                + " which is wrong because it should have been rejected.");
//      } catch (ClientException e) {
//      }
//    } catch (Exception e) {
//      fail("Exception while reading fields in ACLNotReadOtherGuidAllFieldsTest: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_106_ACLNotReadOtherGuidFieldTest() {
//    try {
//      try {
//        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
//                samEntry);
//        fail("Result of read of westy's environment by sam is " + result
//                + " which is wrong because it should have been rejected.");
//      } catch (ClientException e) {
//      }
//    } catch (Exception e) {
//      fail("Exception while reading fields in ACLNotReadOtherGuidFieldTest: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_110_ACLPartOne() {
//    try {
//      try {
//        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
//      } catch (Exception e) {
//        fail("Exception adding Sam to Westy's readlist: " + e);
//        e.printStackTrace();
//      }
//      try {
//        assertEquals("work",
//                client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
//      } catch (Exception e) {
//        fail("Exception while Sam reading Westy's field: " + e);
//        e.printStackTrace();
//      }
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it in ACLPartOne: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_120_ACLPartTwo() {
//    try {
//      String barneyName = "barney" + RandomString.randomString(6);
//      try {
//        client.lookupGuid(barneyName);
//        fail(barneyName + " entity should not exist");
//      } catch (ClientException e) {
//      } catch (Exception e) {
//        fail("Exception looking up Barney: " + e);
//        e.printStackTrace();
//      }
//      barneyEntry = client.guidCreate(masterGuid, barneyName);
//      // remove default read access for this test
//      client.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
//              GNSCommandProtocol.ENTIRE_RECORD, GNSCommandProtocol.ALL_GUIDS);
//      client.fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
//      client.fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);
//
//      try {
//        // let anybody read barney's cell field
//        client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
//                GNSCommandProtocol.ALL_GUIDS);
//      } catch (Exception e) {
//        fail("Exception creating ALLUSERS access for Barney's cell: " + e);
//        e.printStackTrace();
//      }
//
//      try {
//        assertEquals("413-555-1234",
//                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", samEntry));
//      } catch (Exception e) {
//        fail("Exception while Sam reading Barney' cell: " + e);
//        e.printStackTrace();
//      }
//
//      try {
//        assertEquals("413-555-1234",
//                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", westyEntry));
//      } catch (Exception e) {
//        fail("Exception while Westy reading Barney' cell: " + e);
//        e.printStackTrace();
//      }
//
//      try {
//        String result = client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address",
//                samEntry);
//        fail("Result of read of barney's address by sam is " + result
//                + " which is wrong because it should have been rejected.");
//      } catch (ClientException e) {
//        if (e.getCode() == ResponseCode.ACCESS_ERROR) {
//          System.out
//                  .print("This was expected for null querier trying to ReadUnsigned "
//                          + barneyEntry.getGuid()
//                          + "'s address: "
//                          + e);
//        }
//      } catch (Exception e) {
//        fail("Exception while Sam reading Barney' address: " + e);
//        e.printStackTrace();
//      }
//
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it in ACLPartTwo: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_130_ACLALLFields() {
//    //testACL();
//    String superUserName = "superuser" + RandomString.randomString(6);
//    try {
//      try {
//        client.lookupGuid(superUserName);
//        fail(superUserName + " entity should not exist");
//      } catch (ClientException e) {
//      }
//
//      GuidEntry superuserEntry = client.guidCreate(masterGuid, superUserName);
//
//      // let superuser read any of barney's fields
//      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSCommandProtocol.ENTIRE_RECORD, superuserEntry.getGuid());
//
//      assertEquals("413-555-1234",
//              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
//      assertEquals("100 Main Street",
//              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));
//
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it in ACLALLFields: " + e);
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   *
//   */
//  @Test
//  public void test_140_ACLCreateDeeperField() {
//    try {
//      try {
//        client.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
//      } catch (Exception e) {
//        fail("Problem updating field: " + e);
//        e.printStackTrace();
//      }
//      try {
//        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field", GNSCommandProtocol.ENTIRE_RECORD);
//      } catch (Exception e) {
//        fail("Problem adding acl: " + e);
//        e.printStackTrace();
//      }
//      try {
//        JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
//                "test.deeper.field", westyEntry.getGuid());
//        JSONArray expected = new JSONArray(new ArrayList<String>(Arrays.asList(GNSCommandProtocol.ENTIRE_RECORD)));
//        JSONAssert.assertEquals(expected, actual, true);
//      } catch (Exception e) {
//        fail("Problem reading acl: " + e);
//        e.printStackTrace();
//      }
//    } catch (Exception e) {
//      fail("Exception when we were not expecting it ACLCreateDeeperField: " + e);
//      e.printStackTrace();
//    }
//  }
}
