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
import edu.umass.cs.gnsclient.client.util.GuidUtils;

import edu.umass.cs.gnscommon.GNSProtocol;
import java.io.IOException;

import org.json.JSONObject;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateAccountTest {

  private static String ACCOUNT_ALIAS = "support@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;

  /**
   *
   */
  public CreateAccountTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_01_CreateAccount() {
    try {
      GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_CheckAccount() {
    String guidString = null;
    try {
      guidString = client.lookupGuid(ACCOUNT_ALIAS);
    } catch (Exception e) {
      fail("Exception while looking up guid: " + e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = client.lookupAccountRecord(guidString);
      } catch (Exception e) {
        fail("Exception while looking up account record: " + e);
      }
    }
    if (json != null) {
      try {
        assertFalse(json.getBoolean(GNSProtocol.ACCOUNT_RECORD_VERIFIED.toString()));
      } catch (Exception e) {
        fail("Exception while getting field from account record: " + e);
      }
    }
  }

}
