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
import org.json.JSONArray;
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
public class GroupAddTest {

  private static String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry mygroupEntry;
  private static GuidEntry guidToDeleteEntry;

  /**
   *
   */
  public GroupAddTest() {
    if (client == null) {
       try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        failWithStackTrace("Exception creating client: ", e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        failWithStackTrace("Exception when we were not expecting it: ", e);
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
      guidToDeleteEntry = client.guidCreate(masterGuid, "deleteMe" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guids: ", e);
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
        failWithStackTrace(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      
      mygroupEntry = client.guidCreate(masterGuid, mygroupName);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guids: ", e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_211_GroupAdd() {
    try {
      JSONArray guids = new JSONArray(Arrays.asList(westyEntry.getGuid(), samEntry.getGuid(), guidToDeleteEntry.getGuid()));
      client.groupAddGuids(mygroupEntry.getGuid(), guids, mygroupEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while adding to groups: ", e);
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

    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception while getting members and groups: ", e);
    }
  }
  
  private static GuidEntry oneEntry;
  private static GuidEntry twoEntry;
  private static GuidEntry threeEntry;
  private static GuidEntry anotherGroupEntry;
  
  /**
   *
   */
  @Test
  public void test_220_testCreateSecondGuids() {
    try {
      oneEntry = client.guidCreate(masterGuid, "one" + RandomString.randomString(6));
      twoEntry = client.guidCreate(masterGuid, "two" + RandomString.randomString(6));
      threeEntry = client.guidCreate(masterGuid, "three" + RandomString.randomString(6));
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guids: ", e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_221_GroupSecondCreate() {
    String another = "anotherGroupEntry" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(another);
        failWithStackTrace(another + " entity should not exist");
      } catch (ClientException e) {
      }
      
      anotherGroupEntry = client.guidCreate(masterGuid, another);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guids: ", e);
    }
  }


  /**
   *
   */
  @Test
  public void test_222_GroupAddOne() {
    try {
      client.groupAddGuid(anotherGroupEntry.getGuid(), oneEntry.getGuid(), anotherGroupEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while adding One: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_225_GroupAddTwo() {
    try {
      client.groupAddGuid(anotherGroupEntry.getGuid(), twoEntry.getGuid(), anotherGroupEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while adding Two: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_226_GroupAddThree() {
    try {
      client.groupAddGuid(anotherGroupEntry.getGuid(), threeEntry.getGuid(), anotherGroupEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while adding Three: ", e);
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
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(anotherGroupEntry.getGuid(), anotherGroupEntry));
      assertEquals(expected, actual);

      // and that each of the guids is in the right group
      expected = new HashSet<>(Arrays.asList(anotherGroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(oneEntry.getGuid(), oneEntry));
      assertEquals(expected, actual);

      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(twoEntry.getGuid(), twoEntry));
      assertEquals(expected, actual);

      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(threeEntry.getGuid(), threeEntry));
      assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception while getting members and groups: ", e);
    }
  }
  
   private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }


}
