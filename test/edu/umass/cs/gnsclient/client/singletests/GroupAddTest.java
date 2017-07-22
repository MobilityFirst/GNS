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
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import java.util.Arrays;
import java.util.HashSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test adding and removing members from groups.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupAddTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;
  private static GuidEntry oneEntry;
  private static GuidEntry twoEntry;
  private static GuidEntry threeEntry;
  private static GuidEntry anotherGroupEntry;

  /**
   *
   */
  public GroupAddTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: ", e);
      }
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: ", e);
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
      guidToDeleteEntry = clientCommands.guidCreate(masterGuid, "deleteMe" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: ", e);
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

      mygroupEntry = clientCommands.guidCreate(masterGuid, mygroupName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: ", e);
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
      Utils.failWithStackTrace("Exception while adding to groups: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_212_GroupAddCheck() {
    try {
      // Make sure the group has all the right members
      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
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
      Utils.failWithStackTrace("Exception while getting members and groups: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_220_testCreateSecondGuids() {
    try {
      oneEntry = clientCommands.guidCreate(masterGuid, "one" + RandomString.randomString(12));
      twoEntry = clientCommands.guidCreate(masterGuid, "two" + RandomString.randomString(12));
      threeEntry = clientCommands.guidCreate(masterGuid, "three" + RandomString.randomString(12));
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_221_GroupSecondCreate() {
    String another = "anotherGroupEntry" + RandomString.randomString(12);
    try {
      try {
        clientCommands.lookupGuid(another);
        Utils.failWithStackTrace(another + " entity should not exist");
      } catch (ClientException e) {
      }

      anotherGroupEntry = clientCommands.guidCreate(masterGuid, another);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_222_GroupAddOne() {
    try {
      clientCommands.groupAddGuid(anotherGroupEntry.getGuid(), oneEntry.getGuid(), anotherGroupEntry);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while adding One: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_225_GroupAddTwo() {
    try {
      clientCommands.groupAddGuid(anotherGroupEntry.getGuid(), twoEntry.getGuid(), anotherGroupEntry);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while adding Two: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_226_GroupAddThree() {
    try {
      clientCommands.groupAddGuid(anotherGroupEntry.getGuid(), threeEntry.getGuid(), anotherGroupEntry);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while adding Three: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_227_GroupAddCheck() {
    try {
      // Make sure the group has all the right members
      HashSet<String> expected = new HashSet<>(Arrays.asList(oneEntry.getGuid(), twoEntry.getGuid(), threeEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.groupGetMembers(anotherGroupEntry.getGuid(), anotherGroupEntry));
      Assert.assertEquals(expected, actual);

      // and that each of the guids is in the right group
      expected = new HashSet<>(Arrays.asList(anotherGroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(oneEntry.getGuid(), oneEntry));
      Assert.assertEquals(expected, actual);

      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(twoEntry.getGuid(), twoEntry));
      Assert.assertEquals(expected, actual);

      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(threeEntry.getGuid(), threeEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while getting members and groups: ", e);
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
      clientCommands.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
      clientCommands.guidRemove(masterGuid, oneEntry.getGuid());
      clientCommands.guidRemove(masterGuid, twoEntry.getGuid());
      clientCommands.guidRemove(masterGuid, threeEntry.getGuid());
      clientCommands.guidRemove(masterGuid, mygroupEntry.getGuid());
      clientCommands.guidRemove(masterGuid, anotherGroupEntry.getGuid());

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }
}
