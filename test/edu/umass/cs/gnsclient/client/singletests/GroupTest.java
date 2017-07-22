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
public class GroupTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;


  /**
   *
   */
  public GroupTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
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
  public void test_01_testCreateGuids() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
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
  public void test_211_GroupAddWesty() {
    try {
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while adding Westy: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_212_GroupAddSam() {
    try {
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while adding Sam: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_213_GroupAddGuidToDelete() {
    try {
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), guidToDeleteEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while adding GuidToDelete: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_214_GroupAddCheck() {
    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

      expected = new HashSet<>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(westyEntry.getGuid(), westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while getting members and groups: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_215_GroupRemoveGuid() {
    // now remove a guid and check for group updates
    try {
      clientCommands.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
    try {
      clientCommands.lookupGuidRecord(guidToDeleteEntry.getGuid());
      Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_216_GroupRemoveCheck() {
    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception during remove guid group update test: " + e);
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

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }
}
