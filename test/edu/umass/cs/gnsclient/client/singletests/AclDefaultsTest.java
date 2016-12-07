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
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import java.io.IOException;
import java.util.Arrays;

import org.json.JSONArray;

import org.json.JSONObject;
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
        failWithStackTrace("Exception creating client: ", e);
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
      failWithStackTrace("Exception while creating guid: ", e);
    }
  }

  private static final String TEST_FIELD_NAME = "testField";

  /**
   *
   */
  @Test
  public void test_101_ACLCreateField() {
    try {
      client.fieldCreateOneElementList(masterGuid.getGuid(), TEST_FIELD_NAME, "testValue", masterGuid);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating fields in ACLCreateFields: ", e);
      e.printStackTrace();
    }
  }

  //
  // Start with some simple tests to insure that basic ACL mechanics work
  //
  /**
   * Add the ALL_GUID to GNSProtocol.ENTIRE_RECORD.toString() if it's not there already.
   */
  @Test
  public void test_110_ACLMaybeAddAllFields() {
    try {
      if (!JSONUtils.JSONArrayToArrayList(client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()))
              .contains(GNSProtocol.ALL_GUIDS.toString())) {
        client.aclAdd(AclAccessType.READ_WHITELIST, masterGuid,
                GNSProtocol.ENTIRE_RECORD.toString(),
                GNSProtocol.ALL_GUIDS.toString());
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while checking for ALL_FIELDS in ACLMaybeAddAllFields: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_111_ACLCheckForAllFieldsPass() {
    try {
      ThreadUtils.sleep(100);
      JSONArray expected = new JSONArray(Arrays.asList(GNSProtocol.ALL_GUIDS.toString()));
      JSONAssert.assertEquals(expected,
              client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()), true);
    } catch (Exception e) {
      failWithStackTrace("Exception while checking ALL_FIELDS in ACLCheckForAllFieldsPass: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_112_ACLRemoveAllFields() {
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, masterGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing ACL in ACLRemoveAllFields: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_113_ACLCheckForAllFieldsMissing() {
    try {
      JSONArray expected = new JSONArray();
      JSONAssert.assertEquals(expected,
              client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()), true);
    } catch (Exception e) {
      failWithStackTrace("Exception while checking ALL_FIELDS in ACLCheckForAllFieldsMissing: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_114_CheckAllFieldsAcl() {
    try {
      assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString()));
    } catch (Exception e) {
      failWithStackTrace("Exception in CheckAllFieldsAcl: ", e);
    }
  }

  @Test
  public void test_115_DeleteAllFieldsAcl() {
    try {
      client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception in DeleteAllFieldsAcl: ", e);
    }
  }

  @Test
  public void test_116_CheckAllFieldsAclGone() {
    try {
      assertFalse(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString()));
    } catch (Exception e) {
      failWithStackTrace("Exception in CheckAllFieldsAclGone: ", e);
    }
  }

  @Test
  public void test_120_CreateAcl() {
    try {
      client.fieldCreateAcl(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME);
    } catch (Exception e) {
      failWithStackTrace("Exception CreateAcl while creating ACL field: ", e);
    }
  }

  @Test
  public void test_121_CheckAcl() {
    try {
      assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME));
    } catch (Exception e) {
      failWithStackTrace("Exception CheckAcl: ", e);
    }
  }

  @Test
  public void test_122_DeleteAcl() {
    try {
      client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME);
    } catch (Exception e) {
      failWithStackTrace("Exception in DeleteAcl: ", e);
    }
  }

  @Test
  public void test_123_CheckAclGone() {
    try {
      assertFalse(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, TEST_FIELD_NAME));
    } catch (Exception e) {
      failWithStackTrace("Exception in CheckAclGonewhile: ", e);
    }
  }

  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;

  /**
   * Create guids for ACL tests.
   */
  @Test
  public void test_130_ACLCreateGuids() {
    try {
      westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy" + RandomString.randomString(6));
      samEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "sam" + RandomString.randomString(6));
    } catch (Exception e) {
      failWithStackTrace("Exception registering guids in ACLCreateGuids: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_131_ACLRemoveAllFields() {
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      client.aclRemove(AclAccessType.READ_WHITELIST, samEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing ACL in ACLRemoveAllFields: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_132_ACLCreateFields() {
    try {
      client.fieldUpdate(westyEntry.getGuid(), "environment", "work", westyEntry);
      client.fieldUpdate(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      client.fieldUpdate(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      client.fieldUpdate(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating fields in ACLCreateFields: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_135_ACLMaybeAddAllFieldsForMaster() {
    try {
      if (!JSONUtils.JSONArrayToArrayList(client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                      GNSProtocol.ENTIRE_RECORD.toString(), westyEntry.getGuid()))
              .contains(masterGuid.getGuid())) {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry,
                GNSProtocol.ENTIRE_RECORD.toString(),
                masterGuid.getGuid());
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while checking for ALL_FIELDS in ACLMaybeAddAllFields: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_136_ACLMasterReadAllFields() {
    try {
      JSONObject expected = new JSONObject();
      expected.put("environment", "work");
      expected.put("password", "666flapJack");
      expected.put("ssn", "000-00-0000");
      expected.put("address", "100 Hinkledinkle Drive");
      JSONObject actual = new JSONObject(client.fieldRead(westyEntry.getGuid(),
              GNSProtocol.ENTIRE_RECORD.toString(), masterGuid));
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading all fields in ACLReadAllFields: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_137_ACLReadMyFields() {
    try {
      // read my own field
      assertEquals("work",
              client.fieldRead(westyEntry.getGuid(), "environment", westyEntry));
      // read another one of my fields field
      assertEquals("000-00-0000",
              client.fieldRead(westyEntry.getGuid(), "ssn", westyEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLReadMyFields: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_138_ACLNotReadOtherGuidAllFieldsTest() {
    try {
      try {
        String result = client.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), samEntry);
        failWithStackTrace("Result of read of all of westy's fields by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLNotReadOtherGuidAllFieldsTest: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_139_ACLNotReadOtherGuidFieldTest() {
    try {
      try {
        String result = client.fieldRead(westyEntry.getGuid(), "environment",
                samEntry);
        failWithStackTrace("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLNotReadOtherGuidFieldTest: ", e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_140_AddACLTest() {
    try {
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
      } catch (Exception e) {
        failWithStackTrace("Exception adding Sam to Westy's readlist: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_141_CheckACLTest() {
    try {
      try {
        assertEquals("work", client.fieldRead(westyEntry.getGuid(), "environment", samEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Westy's field: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
      e.printStackTrace();
    }
  }

  private static GuidEntry barneyEntry;

  /**
   *
   */
  @Test
  public void test_142_ACLCreateAnotherGuid() {
    try {
      String barneyName = "barney" + RandomString.randomString(6);
      try {
        client.lookupGuid(barneyName);
        failWithStackTrace(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (Exception e) {
        failWithStackTrace("Exception looking up Barney: ", e);
        e.printStackTrace();
      }
      barneyEntry = client.guidCreate(masterGuid, barneyName);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_143_ACLAdjustACL() {
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_144_ACLCreateFields() {
    try {
      // remove default read access for this test
      client.fieldUpdate(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldUpdate(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_145_ACLUpdateACL() {
    try {
      try {
        // let anybody read barney's cell field
        client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (Exception e) {
        failWithStackTrace("Exception creating ALL_GUIDS access for Barney's cell: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_146_ACLTestReadsOne() {
    try {
      try {
        assertEquals("413-555-1234",
                client.fieldRead(barneyEntry.getGuid(), "cell", samEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Barney' cell: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_147_ACLTestReadsTwo() {
    try {
      try {
        assertEquals("413-555-1234",
                client.fieldRead(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Westy reading Barney' cell: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLTestReadsTwo: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_148_ACLTestReadsThree() {
    try {
      try {
        String result = client.fieldRead(barneyEntry.getGuid(), "address",
                samEntry);
        failWithStackTrace("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        if (e.getCode() == ResponseCode.ACCESS_ERROR) {
          System.out.print("This was expected for null querier trying to ReadUnsigned "
                  + barneyEntry.getGuid()
                  + "'s address: "
                 + e);
        }
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Barney' address: ", e);
        e.printStackTrace();
      }

    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLTestReadsThree: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_149_ACLALLFields() {
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = client.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      assertEquals("413-555-1234",
              client.fieldRead(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              client.fieldRead(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_150_ACLCreateDeeperField() {
    try {
      try {
        client.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (Exception e) {
        failWithStackTrace("Problem updating field: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_151_ACLAddDeeperFieldACL() {
    try {
      try {
        // Create an empty ACL, effectively disabling access except by the guid itself.
        client.fieldCreateAcl(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field");
      } catch (Exception e) {
        failWithStackTrace("Problem adding acl: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_152_ACLCheckDeeperFieldACLExists() {
    try {
      try {
        assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field"));
      } catch (Exception e) {
        failWithStackTrace("Problem reading acl: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  // This should pass even though the ACL for test.deeper.field is empty because you
  // can always read your own fields.
  @Test
  public void test_153_ACLReadDeeperFieldSelf() {
    try {
      try {
        assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper.field", westyEntry));
      } catch (Exception e) {
        failWithStackTrace("Problem adding read field: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  // This should fail because the ACL for test.deeper.field is empty.
  @Test
  public void test_154_ACLReadDeeperFieldOtherFail() {
    try {
      try {
        assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper.field", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  // This should fail because the ACL for test.deeper.field is empty.
  @Test
  public void test_156_ACLReadShallowFieldOtherFail() {
    try {
      try {
        assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_157_AddAllRecordACL() {
    try {
      client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test", GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  // This should still fail because the ACL for test.deeper.field is empty even though test 
  // now has an GNSProtocol.ALL_GUIDS.toString() at the root (this is different than the old model).
  @Test
  public void test_158_ACLReadDeeperFieldOtherFail() {
    try {
      try {
        assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper.field", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
      e.printStackTrace();
    }
  }

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }

}
