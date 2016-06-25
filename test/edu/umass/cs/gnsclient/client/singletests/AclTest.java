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
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AclTest {

  private static final String ACCOUNT_ALIAS = "test@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;

  public AclTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
    }
  }

  @Test
  public void test_01_CreateField() {
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
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_02_ACLPartOne() {
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
      fail("Exception when we were not expecting it: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_03_ACLPartTwo() {
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
      barneyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, barneyName);
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
      fail("Exception when we were not expecting it: " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void test_04_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        fail(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSCommandProtocol.ALL_FIELDS, superuserEntry.getGuid());

      assertEquals("413-555-1234",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_05_CreateDeeperField() {
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
        JSONArray expected = new JSONArray(new ArrayList(Arrays.asList(GNSCommandProtocol.ALL_FIELDS)));
        JSONAssert.assertEquals(expected, actual, true);
      } catch (Exception e) {
        fail("Problem reading acl: " + e);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

}
