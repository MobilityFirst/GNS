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

import edu.umass.cs.utils.Utils;
import java.io.IOException;

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
public class GroupSingleAddTest {

  private static String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;


  /**
   *
   */
  public GroupSingleAddTest() {
    if (client == null) {
       try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: ", e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
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
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
      samEntry = client.guidCreate(masterGuid, "sam" + RandomString.randomString(6));
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
    String mygroupName = "mygroup" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(mygroupName);
        Utils.failWithStackTrace(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      
      mygroupEntry = client.guidCreate(masterGuid, mygroupName);
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
      client.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
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
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

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
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(westyEntry.getGuid(), westyEntry));
      assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception while getting groups: ", e);
    }
  }
}
