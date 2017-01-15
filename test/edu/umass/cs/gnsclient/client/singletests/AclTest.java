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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
public class AclTest extends DefaultGNSTest {

//  private static final String ACCOUNT_ALIAS = "test@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
//  private static final String PASSWORD = "password";
  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;

  
  /**
   *
   */
  public AclTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
        //GuidUtils.lookupOrCreateAccountGuid(clientCommands, ACCOUNT_ALIAS, PASSWORD, true);

      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_100_ACLCreateGuids() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception registering guids in ACLCreateGuids: " + e);
      e.printStackTrace();
    }
    try {
      // remove default read access for this test
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      fail("Exception while removing ACL in ACLCreateGuids: " + e);
      e.printStackTrace();
    }
    try {
      JSONArray expected = new JSONArray(new ArrayList<String>(Arrays.asList(masterGuid.getGuid())));
      JSONArray actual = clientCommands.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), westyEntry.getGuid());
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      fail("Exception while retrieving ACL in ACLCreateGuids: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_101_ACLCreateFields() {
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
    } catch (Exception e) {
      fail("Exception while creating fields in ACLCreateFields: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_102_ACLReadAllFields() {
    try {
      JSONObject expected = new JSONObject();
      expected.put("environment", new JSONArray(new ArrayList<String>(Arrays.asList("work"))));
      expected.put("password", new JSONArray(new ArrayList<String>(Arrays.asList("666flapJack"))));
      expected.put("ssn", new JSONArray(new ArrayList<String>(Arrays.asList("000-00-0000"))));
      expected.put("address", new JSONArray(new ArrayList<String>(Arrays.asList("100 Hinkledinkle Drive"))));
      JSONObject actual = new JSONObject(clientCommands.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), masterGuid));
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      fail("Exception while reading all fields in ACLReadAllFields: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_104_ACLReadMyFields() {
    try {
      // read my own field
      assertEquals("work",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another one of my fields field
      assertEquals("000-00-0000",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));

    } catch (Exception e) {
      fail("Exception while reading fields in ACLReadMyFields: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_105_ACLNotReadOtherGuidAllFieldsTest() {
    try {
      try {
        String result = clientCommands.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), samEntry);
        fail("Result of read of all of westy's fields by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while reading fields in ACLNotReadOtherGuidAllFieldsTest: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_106_ACLNotReadOtherGuidFieldTest() {
    try {
      try {
        String result = clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
                samEntry);
        fail("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while reading fields in ACLNotReadOtherGuidFieldTest: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_110_ACLPartOne() {
    try {
      try {
        clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
      } catch (Exception e) {
        fail("Exception adding Sam to Westy's readlist: " + e);
        e.printStackTrace();
      }
      try {
        assertEquals("work",
                clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
      } catch (Exception e) {
        fail("Exception while Sam reading Westy's field: " + e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it in ACLPartOne: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_120_ACLPartTwo() {
    try {
      String barneyName = "barney" + RandomString.randomString(6);
      try {
        clientCommands.lookupGuid(barneyName);
        fail(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (Exception e) {
        fail("Exception looking up Barney: " + e);
        e.printStackTrace();
      }
      barneyEntry = clientCommands.guidCreate(masterGuid, barneyName);
      // remove default read access for this test
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      clientCommands.fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      clientCommands.fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);

      try {
        // let anybody read barney's cell field
        clientCommands.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (Exception e) {
        fail("Exception creating ALLUSERS access for Barney's cell: " + e);
        e.printStackTrace();
      }

      try {
        assertEquals("413-555-1234",
                clientCommands.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", samEntry));
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' cell: " + e);
        e.printStackTrace();
      }

      try {
        assertEquals("413-555-1234",
                clientCommands.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (Exception e) {
        fail("Exception while Westy reading Barney' cell: " + e);
        e.printStackTrace();
      }

      try {
        String result = clientCommands.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address",
                samEntry);
        fail("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        if (e.getCode() == ResponseCode.ACCESS_ERROR) {
          System.out
                  .print("This was expected for null querier trying to ReadUnsigned "
                          + barneyEntry.getGuid()
                          + "'s address: "
                          + e);
        }
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' address: " + e);
        e.printStackTrace();
      }

    } catch (Exception e) {
      fail("Exception when we were not expecting it in ACLPartTwo: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_130_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        clientCommands.lookupGuid(superUserName);
        fail(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = clientCommands.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      assertEquals("413-555-1234",
              clientCommands.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              clientCommands.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      fail("Exception when we were not expecting it in ACLALLFields: " + e);
      e.printStackTrace();
    }
  }

  /**
   *
   */
  @Test
  public void test_140_ACLCreateDeeperField() {
    try {
      try {
        clientCommands.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (Exception e) {
        fail("Problem updating field: " + e);
        e.printStackTrace();
      }
      try {
        clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field", GNSProtocol.ENTIRE_RECORD.toString());
      } catch (Exception e) {
        fail("Problem adding acl: " + e);
        e.printStackTrace();
      }
      try {
        JSONArray actual = clientCommands.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                "test.deeper.field", westyEntry.getGuid());
        JSONArray expected = new JSONArray(new ArrayList<String>(Arrays.asList(GNSProtocol.ENTIRE_RECORD.toString())));
        JSONAssert.assertEquals(expected, actual, true);
      } catch (Exception e) {
        fail("Problem reading acl: " + e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it ACLCreateDeeperField: " + e);
      e.printStackTrace();
    }
  }
}
