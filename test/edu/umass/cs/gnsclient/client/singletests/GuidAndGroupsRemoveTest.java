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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Functionality test for core elements in the client using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GuidAndGroupsRemoveTest {

  private static String accountAlias = "test@cgns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static String password = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;

  public GuidAndGroupsRemoveTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      if (System.getProperty("alias") != null
              && !System.getProperty("alias").isEmpty()) {
        accountAlias = System.getProperty("alias");
      }
      if (System.getProperty("password") != null
              && !System.getProperty("password").isEmpty()) {
        password = System.getProperty("password");
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, accountAlias, password, true);
      } catch (Exception e) {
        fail("Exception while creating account guid: " + e);
      }
    }
  }

  @Test
  public void test_10_CreateGuids() {
    try {
      westyEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "westy" + RandomString.randomString(6));
      samEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception registering guids for create fields: " + e);
    }
  }

  @Test
  public void test_210_GroupCreate() {
    String mygroupName = "mygroup" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(mygroupName);
        fail(mygroupName + " entity should not exist");
      } catch (ClientException e) {
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
    } catch (Exception e) {
      fail("Exception while adding to groups: " + e);
    }
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

      expected = new HashSet<String>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(westyEntry.getGuid(), westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception while getting members and groups: " + e);
    }
  }

  @Test
  public void test_212_GroupRemoveGuid() {
    // now remove a guid and check for group updates
    try {
      client.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }

  @Test
  public void test_213_GroupRemoveCheck() {
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception during remove guid group update test: " + e);
      System.exit(2);
    }
  }

  @Test
  public void test_999_Stop() {
    try {
      client.close();
    } catch (Exception e) {
      fail("Exception during stop: " + e);
    }
  }
}
