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
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.hamcrest.Matchers;
import org.json.JSONArray;
import org.json.JSONException;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test groups and ACLs.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupAndAclTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;
  private static GuidEntry groupAccessUserEntry = null;

  private static final int COORDINATION_WAIT = 100;

  private static void waitSettle() {
    try {
      Thread.sleep(COORDINATION_WAIT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   *
   */
  public GroupAndAclTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while creating account guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_01_testCreateGuids() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_210_GroupCreate() {
    String mygroupName = "mygroup" + RandomString.randomString(12);
    try {
      try {
        clientCommands.lookupGuid(mygroupName);
        Utils.failWithStackTrace(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = clientCommands.guidCreate(masterGuid, "deleteMe" + RandomString.randomString(12));
      mygroupEntry = clientCommands.guidCreate(masterGuid, mygroupName);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_211_GroupAdd() {
    try {
      JSONArray guids = new JSONArray(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      clientCommands.groupAddGuids(mygroupEntry.getGuid(), guids, mygroupEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while adding to groups: " + e);
    }
    waitSettle();
  }

  /**
   *
   */
  @Test
  public void test_212_GroupAddCheck() {
    try {
      // Make sure the group has all the right members
      HashSet<String> expected = new HashSet<>(
              Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(
              clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

      // and that each of the guids is in the right group
      expected = new HashSet<>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(westyEntry.getGuid(), westyEntry));
      Assert.assertEquals(expected, actual);

      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(samEntry.getGuid(), samEntry));
      Assert.assertEquals(expected, actual);

      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(guidToDeleteEntry.getGuid(), guidToDeleteEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while getting members and groups: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_213_GroupRemoveGuid() {
    // now remove a guid and check for group updates
    try {
      clientCommands.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
    waitSettle();
  }

  /**
   *
   */
  @Test
  public void test_214_GroupRemoveGuidCheck() {
    try {
      clientCommands.lookupGuidRecord(guidToDeleteEntry.getGuid());
      Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {
      // normal result
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }

  }

  /**
   *
   */
  @Test
  public void test_215_GroupRemoveCheck() {
    try {
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(
              clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertThat(actual, Matchers.not(Matchers.hasItem(guidToDeleteEntry.getGuid())));

    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception during remove guid group update test: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_220_GroupAndACLCreateGuids() {
    String groupAccessUserName = "groupAccessUser" + RandomString.randomString(12);
    try {
      try {
        clientCommands.lookupGuid(groupAccessUserName);
        Utils.failWithStackTrace(groupAccessUserName + " entity should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Checking for existence of group user: " + e);
    }

    try {
      groupAccessUserEntry = clientCommands.guidCreate(masterGuid, groupAccessUserName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception creating group user: " + e);
    }
    waitSettle();
  }

  /**
   *
   */
  @Test
  public void test_221_GroupAndACLCreateGuidsRemoveAll() {
    try {
      // remove all fields read by all
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception creating group user: " + e);
    }
    waitSettle();
  }

  /**
   *
   */
  @Test
  public void test_222_GroupAndACLCreateGuidsRemoveAllCheck() {
    try {
      // test of remove all fields read by all
      JSONAssert.assertEquals(new JSONArray(Arrays.asList(masterGuid.getGuid())), clientCommands.aclGet(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), groupAccessUserEntry.getGuid()), JSONCompareMode.STRICT);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception test acl: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_223_GroupAndACLCreateGuids() {
    try {
      clientCommands.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "address", "23 Jumper Road", groupAccessUserEntry);
      clientCommands.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "age", "43", groupAccessUserEntry);
      clientCommands.fieldCreateOneElementList(groupAccessUserEntry.getGuid(), "hometown", "whoville", groupAccessUserEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception creating group user fields: " + e);
    }
    try {
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, groupAccessUserEntry, "hometown", mygroupEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception adding mygroup to acl for group user hometown field: " + e);
    }
    waitSettle();
  }

  /**
   *
   */
  @Test
  public void test_224_GroupAndACLTestBadAccess() {
    try {
      try {
        String result = clientCommands.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "address", westyEntry);
        Utils.failWithStackTrace("Result of read of groupAccessUser's age by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while attempting a Utils.failWithStackTraceing read of groupAccessUser's age by sam: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_225_GroupAndACLTestGoodAccess() {
    try {
      Assert.assertEquals("whoville", clientCommands.fieldReadArrayFirstElement(groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while attempting read of groupAccessUser's hometown by westy: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_226_GroupAndACLTestRemoveGuid() {

    try {
      clientCommands.groupRemoveGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception removing westy from mygroup: " + e);
    }
    waitSettle();
  }

  /**
   *
   */
  @Test
  public void test_227_GroupAndACLTestRemoveGuidCheck() {
    try {
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertThat(actual, Matchers.not(Matchers.hasItem(westyEntry.getGuid())));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_230_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
      clientCommands.guidRemove(masterGuid, samEntry.getGuid());
      clientCommands.guidRemove(masterGuid, mygroupEntry.getGuid());
      clientCommands.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
      clientCommands.guidRemove(masterGuid, groupAccessUserEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }
}
