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
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.HashSet;

import org.hamcrest.Matchers;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test the alias functionality.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AliasTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static final String alias = "ALIAS-" + RandomString.randomString(12) + "@blah.org";

  /**
   *
   */
  public AliasTest() {
    if (clientCommands == null) {
      try {
        clientCommands = (GNSClientCommands)new GNSClientCommands().setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_01_CreateAccount() {
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_AliasAdd() {
    try {
      //
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      //
      // add an alias to the masterGuid
      clientCommands.addAlias(masterGuid, alias);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we adding alias: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_AliasAddCheck() {
    try {
      // lookup the guid using the alias
      Assert.assertEquals(masterGuid.getGuid(), clientCommands.lookupGuid(alias));

      // grab all the aliases from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands.getAliases(masterGuid));
      // make sure our new one is in there
      Assert.assertThat(actual, Matchers.hasItem(alias));

    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_04_AliasRemove() {
    try {
      // now remove it 
      clientCommands.removeAlias(masterGuid, alias);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing alias: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_05_AliasRemoveCheck() {
    int cnt = 0;
    try {
      do {
        try {
          clientCommands.lookupGuid(alias);
          if (cnt++ > 10) {
            Utils.failWithStackTrace(alias + " should not exist (after 10 checks)");
            break;
          }

        } catch (IOException e) {
          Utils.failWithStackTrace("Exception while looking up alias: " + e);
        }
        ThreadUtils.sleep(10);
      } while (true);
    } catch (ClientException e) {
      System.out.println(alias + " was gone on " + (cnt + 1) + " read");
    }
  }
}
