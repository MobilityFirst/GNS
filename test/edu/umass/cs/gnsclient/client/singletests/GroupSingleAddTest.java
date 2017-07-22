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
public class GroupSingleAddTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;

  /**
   *
   */
  public GroupSingleAddTest() {
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
  public void test_200_testCreateGuids() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
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
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating guids: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_211_GroupAdd() {
    try {
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while adding to groups: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_212_GroupAddCheckMembers() {
    try {
      // Make sure the group has all the right members
      HashSet<String> expected = new HashSet<>(Arrays.asList(westyEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while getting members: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_213_GroupAddCheckGroups() {
    try {
      // and that each of the guids is in the right group
      HashSet<String> expected = new HashSet<>(Arrays.asList(mygroupEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(westyEntry.getGuid(), westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while getting groups: ", e);
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
