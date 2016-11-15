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

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RemoveGuidTestNoReadCoordination {

  private static String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  protected static GNSClient client;
  private static GuidEntry masterGuid;
  private static GuidEntry guidToDeleteEntry;

  /**
   *
   */
  public RemoveGuidTestNoReadCoordination() {
    if (client == null) {
      try {
        client = new GNSClient();
      } catch (IOException e) {
        fail("Exception while creating client: " + e);
      }
    }
  }
  
  /**
   *
   */
  @Test
  public void test_209_testCreateAccountGuid() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      fail("Exception while creating account guid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_210_GuidCreate() {
    String deleteName = "deleteMe" + RandomString.randomString(6);
    try {
      try {
        client.execute(GNSCommand.lookupGUID(deleteName));
        fail(deleteName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, deleteName);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
  }

   @Test
  public void test_212_GuidCreateCheck() {
    try {
      client.execute(GNSCommand.lookupGUIDRecord(guidToDeleteEntry.getGuid()));
    } catch (ClientException | IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_214_GroupRemoveGuid() {
    try {
      client.execute(GNSCommand.removeGUID(masterGuid, guidToDeleteEntry.getGuid()));
    } catch (Exception e) {
      fail("Exception while removing testGuid: " + e);
    }
  }
  
  @Test
  public void test_216_GroupRemoveGuidCheck() {
    try {
      client.execute(GNSCommand.lookupGUIDRecord(guidToDeleteEntry.getGuid()));
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: " + e);
    }
  }
}
