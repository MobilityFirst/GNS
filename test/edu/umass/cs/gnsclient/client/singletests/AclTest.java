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
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;

import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AclTest extends DefaultGNSTest {

  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;

  
  /**
   * This test tests all manner of ACL uses.
   */
  public AclTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
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
  public void test_100_ACLCreateGuids() {
    try {
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = client.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception registering guids in ACLCreateGuids: " + e);

    }
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing ACL in ACLCreateGuids: " + e);

    }
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(masterGuid.getGuid())));
      JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), westyEntry.getGuid());
      JSONAssert.assertEquals(expected, actual, true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while retrieving ACL in ACLCreateGuids: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_101_ACLCreateFields() {
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating fields in ACLCreateFields: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_102_ACLReadAllFields() {
    try {
      JSONObject expected = new JSONObject();
      expected.put("environment", new JSONArray(new ArrayList<>(Arrays.asList("work"))));
      expected.put("password", new JSONArray(new ArrayList<>(Arrays.asList("666flapJack"))));
      expected.put("ssn", new JSONArray(new ArrayList<>(Arrays.asList("000-00-0000"))));
      expected.put("address", new JSONArray(new ArrayList<>(Arrays.asList("100 Hinkledinkle Drive"))));
      JSONObject actual = new JSONObject(client.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), masterGuid));
      JSONAssert.assertEquals(expected, actual, true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while reading all fields in ACLReadAllFields: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_104_ACLReadMyFields() {
    try {
      // read my own field
      Assert.assertEquals("work",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another one of my fields field
      Assert.assertEquals("000-00-0000",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while reading fields in ACLReadMyFields: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_105_ACLNotReadOtherGuidAllFieldsTest() {
    try {
      try {
        String result = client.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), samEntry);
        Utils.failWithStackTrace("Result of read of all of westy's fields by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        // normal result
      }
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while reading fields in ACLNotReadOtherGuidAllFieldsTest: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_106_ACLNotReadOtherGuidFieldTest() {
    try {
      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
                samEntry);
        Utils.failWithStackTrace("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while reading fields in ACLNotReadOtherGuidFieldTest: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_110_ACLPartOne() {
    try {
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception adding Sam to Westy's readlist: " + e);

      }
      try {
        Assert.assertEquals("work",
                client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while Sam reading Westy's field: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLPartOne: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_120_ACLPartTwo() {
    try {
      String barneyName = "barney" + RandomString.randomString(12);
      try {
        client.lookupGuid(barneyName);
        Utils.failWithStackTrace(barneyName + " entity should not exist");
      } catch (ClientException e) {
        // Normal result
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception looking up Barney: " + e);

      }
      barneyEntry = client.guidCreate(masterGuid, barneyName);
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);

      try {
        // let anybody read barney's cell field
        client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception creating ALLUSERS access for Barney's cell: " + e);

      }

      try {
        Assert.assertEquals("413-555-1234",
                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", samEntry));
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while Sam reading Barney' cell: " + e);

      }

      try {
        Assert.assertEquals("413-555-1234",
                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while Westy reading Barney' cell: " + e);

      }

      try {
        String result = client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address",
                samEntry);
        Utils.failWithStackTrace("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        if (e.getCode() == ResponseCode.ACCESS_ERROR) {
          System.out
                  .print("This was expected for null querier trying to ReadUnsigned "
                          + barneyEntry.getGuid()
                          + "'s address: "
                          + e);
        }
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while Sam reading Barney' address: " + e);

      }

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_130_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(12);
    try {
      try {
        client.lookupGuid(superUserName);
        Utils.failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = client.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      Assert.assertEquals("413-555-1234",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      Assert.assertEquals("100 Main Street",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));
      try {
        client.guidRemove(masterGuid, superuserEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing superuserEntry in  ACLALLFields: " + e);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_140_ACLCreateDeeperField() {
    try {
      try {
        client.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem updating field: " + e);

      }
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field", GNSProtocol.ALL_GUIDS.toString());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem adding acl: " + e);

      }
      try {
        JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                "test.deeper.field", westyEntry.getGuid());
        JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(GNSProtocol.ALL_GUIDS.toString())));
        JSONAssert.assertEquals(expected, actual, true);
      } catch (ClientException | IOException | JSONException e) {
        Utils.failWithStackTrace("Problem reading acl: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: " + e);

    }
  }

  /**
   * Remove from the CL
   */
  @Test
  public void test_142_ACLRemoveDeeperFieldACL() {
    try {
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry,
              "test.deeper.field", GNSProtocol.ALL_GUIDS.toString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLRemoveDeeperFieldACL: " + e);

    }
  }

  /**
   * Check that the ACL is empty
   */
  @Test
  public void test_144_ACLCheckDeeperFieldEmpty() {
    try {
      JSONArray expected = new JSONArray();
      JSONAssert.assertEquals(expected,
              client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                      "test.deeper.field", masterGuid.getGuid()), true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLCheckDeeperFieldEmpty: " + e);

    }
  }
  
  /**
   * Remove from the CL
   */
  @Test
  public void test_145_ACLRemoveDeeperFieldACLAgain() {
    try {
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry,
              "test.deeper.field", GNSProtocol.ALL_GUIDS.toString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLRemoveDeeperFieldACL: " + e);

    }
  }

  /**
   * Check that the ACL exists
   */
  @Test
  public void test_146_ACLCheckDeeperFieldACLExists() {
    try {
      Assert.assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, westyEntry,
              "test.deeper.field"));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLCheckDeeperFieldACLExists: " + e);

    }
  }

  /**
   * Delete the ACL
   */
  @Test
  public void test_148_ACLDeleteDeeperFieldACL() {
    try {
      try {
        client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field");
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem deleting acl: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLDeleteDeeperFieldACL: " + e);

    }
  }

  /**
   * Check that the ACL does not exist
   */
  @Test
  public void test_150_CheckDeeperFieldACLExists() {
    try {
      try {
        Assert.assertFalse(client.fieldAclExists(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field"));
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem checking acl exists: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLDeleteDeeperFieldACL: " + e);

    }
  }

  /**
   * Try to delete it again
   */
  @Test
  public void test_152_ACLDeleteDeeperFieldACLAgain() {
    try {
      try {
        client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field");
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem deleting acl: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLDeleteDeeperFieldACL: " + e);

    }
  }
  
  /**
   * Try to delete it again
   */
  @Test
  public void test_154_ACLRemoveFromNonexistantField() {
    try {
      try {
        client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, "NonexistantField", 
                GNSProtocol.ALL_GUIDS.toString());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem removing acl: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLRemoveFromNonexistantField: " + e);

    }
  }

  
  /**
   * Try to delete it again
   */
  @Test
  public void test_156_ACLDeleteNonexistantField() {
    try {
      try {
        client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, westyEntry, "NonexistantField");
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Problem deleting acl: " + e);

      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it ACLDeleteNonexistantField: " + e);

    }
  }

  /**
   *
   */
  @Test
  public void test_999_ACLTestCleanup() {
    try {
      client.guidRemove(masterGuid, barneyEntry.getGuid());
      client.guidRemove(masterGuid, westyEntry.getGuid());
      client.guidRemove(masterGuid, samEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }
}
