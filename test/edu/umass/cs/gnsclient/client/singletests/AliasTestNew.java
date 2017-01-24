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
public class AliasTestNew extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public AliasTestNew() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: ", e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_100_CreateGuid() {
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guid: ", e);
    }
  }

  private static String alias = "ALIAS-" + RandomString.randomString(12)
          + "@blah.org";

  /**
   * Add an alias.
   */
  @Test
  public void test_230_AliasAdd() {
    //CHECKED FOR VALIDITY
    try {
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      // add an alias to the masterGuid
      clientCommands.addAlias(masterGuid, alias);
      // lookup the guid using the alias
      Assert.assertEquals(masterGuid.getGuid(), clientCommands.lookupGuid(alias));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while adding alias: ", e);
    }
  }

  /**
   * Test that recently added alias is present.
   */
  @Test
  public void test_231_AliasIsPresent() {
    //CHECKED FOR VALIDITY
    try {
      // grab all the alias from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .getAliases(masterGuid));
      // make sure our new one is in there
      Assert.assertThat(actual, Matchers.hasItem(alias));
      // now remove it
      clientCommands.removeAlias(masterGuid, alias);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception removing alias: ", e);
    }
  }

  /**
   * Check that removed alias is gone.
   */
  @Test
  public void test_232_AliasCheckRemove() {
    //CHECKED FOR VALIDITY
    try {
      // and make sure it is gone
      try {
        clientCommands.lookupGuid(alias);
        Utils.failWithStackTrace(alias + " should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while checking alias: ", e);
    }
  }

}
