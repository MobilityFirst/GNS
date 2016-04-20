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
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import org.json.JSONException;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupAndAclTest {

  private static String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GnsClient client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;

  private static final int COORDINATION_WAIT = 100;

  private static void waitSettle() {
    try {
      Thread.sleep(COORDINATION_WAIT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public GroupAndAclTest() {
    if (client == null) {
      if (System.getProperty("host") != null
              && !System.getProperty("host").isEmpty()
              && System.getProperty("port") != null
              && !System.getProperty("port").isEmpty()) {
        address = new InetSocketAddress(System.getProperty("host"),
                Integer.parseInt(System.getProperty("port")));
      } else {
        address = new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
      }
      try {
        client = new GnsClient(//address, 
                System.getProperty("disableSSL").equals("true"));
      } catch (IOException e) {
        fail("Exception while creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception while creating account guid: " + e);
      }
    }
  }

  @Test
  public void test_01_testCreateGuids() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      samEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
  }

  private static GuidEntry guidToDeleteEntry;

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
    } catch (IOException | GnsClientException e) {
      fail("Exception while adding to groups: " + e);
    }
    try {
      // Make sure the group has all the right members
      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

      // and that each of the guids is in the right group
      expected = new HashSet<>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(westyEntry.getGuid(), westyEntry));
      assertEquals(expected, actual);
      
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(samEntry.getGuid(), samEntry));
      assertEquals(expected, actual);
      
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(guidToDeleteEntry.getGuid(), guidToDeleteEntry));
      assertEquals(expected, actual);

    } catch (IOException | GnsClientException | JSONException e) {
      fail("Exception while getting members and groups: " + e);
    }
  }

  @Test
  public void test_212_GroupRemoveGuid() {
    waitSettle();
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

  @Test
  public void test_213_GroupRemoveCheck() {
    waitSettle();
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception during remove guid group update test: " + e);
      System.exit(2);
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
  public void test_223_GroupAndACLTestRemoveGuid() {

    try {
      try {
        client.groupRemoveGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      } catch (Exception e) {
        fail("Exception removing westy from mygroup: " + e);
      }

      waitSettle();

      HashSet<String> expected = new HashSet<String>(Arrays.asList(samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }
}
