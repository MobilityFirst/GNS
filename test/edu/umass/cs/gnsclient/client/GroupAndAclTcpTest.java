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

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.GnsProtocol.AccessType;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupAndAclTcpTest {

  private static String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;

  public GroupAndAclTcpTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
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
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_02_testGroup() {
    String mygroupName = "mygroup" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(mygroupName);
        fail(mygroupName + " entity should not exist");
      } catch (GnsException e) {
      }
      mygroupEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, mygroupName);

      client.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(), mygroupEntry);

      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
      System.exit(2);
    }
  }

  @Test
  public void test_03_testGroupAndACL() {
    String groupAccessUserName = "groupAccessUser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(groupAccessUserName);
        fail(groupAccessUserName + " entity should not exist");
      } catch (GnsException e) {
      }
      GuidEntry groupAccessUserEntry;
      try {
        groupAccessUserEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupAccessUserName);
        } catch (Exception e) {
        fail("Exception creating group user: " + e);
        return;
      }
      try {
        // remove all fields read by all
        client.aclRemove(AccessType.READ_WHITELIST, groupAccessUserEntry, GnsProtocol.ALL_FIELDS, GnsProtocol.ALL_USERS);
      } catch (Exception e) {
        fail("Exception removing all fields access for " + groupAccessUserEntry.getEntityName() + " : " + e);
        return;
      }
      try {
        client.fieldCreateOneElementList(groupAccessUserEntry, "address", "23 Jumper Road");
        client.fieldCreateOneElementList(groupAccessUserEntry, "age", "43");
        client.fieldCreateOneElementList(groupAccessUserEntry, "hometown", "whoville");
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
        String result = client.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "address", westyEntry);
        fail("Result of read of groupAccessUser's address by westy is " + result
                + " which is wrong because it should have been rejected.");
      } catch (GnsException e) {
      }

      try {
        assertEquals("whoville", client.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "hometown", westyEntry));
      } catch (Exception e) {
        fail("Exception accessing \"hometown\" field: " + e);
      }
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
}
