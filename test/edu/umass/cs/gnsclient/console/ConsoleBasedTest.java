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
 *  Initial developer(s): Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.console;

import java.io.IOException;
import java.io.StringWriter;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import jline.ConsoleReader;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import java.io.ByteArrayInputStream;
import static org.junit.Assert.fail;

/**
 * This class defines a ConsoleBasedTest
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
  public class ConsoleBasedTest {

  private static final String GNS_CLI_PROMPT = "GNS CLI";
  private static final String ACCOUNT = "test_account@localhost";
  private static final String PASSWORD = "password";
  private static final String GROUP_GUID = "test_group";
  private static final String GUID1 = "test_guid1";
  private static final String GUID2 = "test_guid2";
  private static final String FIELD1 = "test_field1";
  private static final String VALUE1 = "1234567890";
  private static final String VALUE2 = "value2";

  /**
   * Used to access the test method name in output messages
   */
  @Rule
  public TestName testName = new TestName();

  /**
   * Test account GUID creation and deletion
   */
  @Test
  public void testAccountGuidCreateDelete() {
    String inCommands;
    String expectedOutput;

    inCommands = "account_create " + ACCOUNT + " " + PASSWORD + "\n";
    //inCommands += "account_verify " + ACCOUNT + " " + createVerificationCode(ACCOUNT) + "\n";
    inCommands += "account_delete " + ACCOUNT + "\n";

    expectedOutput = "Created an account with GUID .*\n";
    //expectedOutput += ACCOUNT + " is not verified.\n";
    //expectedOutput += "Account verified.\n";
    expectedOutput += "Removed account GUID .*\n";

    runCommandsInConsole(inCommands, expectedOutput, true, false);
  }

  /**
   * Repeat testAccountGuidCreateDelete 5 times.
   */
  @Test
  public void test5AccountGuidCreateDelete() {
    for (int i = 0; i < 5; i++) {
      testAccountGuidCreateDelete();
    }
  }

  /**
   * Create an account GUID and set it as default using guid_use
   */
  @Test
  public void testCreateAndSetDefaultGuid() {
    String inCommands;
    String expectedOutput;

    inCommands = "account_create " + ACCOUNT + " " + PASSWORD + "\n";
    //inCommands += "account_verify " + ACCOUNT + " " + createVerificationCode(ACCOUNT) + "\n";
    inCommands += "set_default_guid " + ACCOUNT + "\n";

    expectedOutput = "Created an account with GUID .*\n";
    //expectedOutput += ACCOUNT + " is not verified.\n";
    //expectedOutput += "Account verified.\n";
    expectedOutput += "Looking up alias " + ACCOUNT + " GUID and certificates...\n";
    expectedOutput += "Default GUID set to " + ACCOUNT + "\n";

    runCommandsInConsole(inCommands, expectedOutput, true, false);
  }

  /**
   * Test GUID creation and deletion
   */
  @Test
  public void testField() {
    String inCommands;
    String expectedOutput;

    inCommands = "field_create " + FIELD1 + "\n";
    inCommands += "field_write " + FIELD1 + " " + VALUE1 + "\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_delete " + FIELD1 + "\n";
    inCommands += "field_create " + FIELD1 + " zero\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_append " + FIELD1 + " one\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_set 1 " + FIELD1 + " two\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_clear " + FIELD1 + "\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_delete " + FIELD1 + "\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_write " + FIELD1 + " " + VALUE1 + "\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_write " + FIELD1 + " " + VALUE2 + "\n";
    inCommands += "field_read " + FIELD1 + "\n";
    inCommands += "field_delete " + FIELD1 + "\n";

    expectedOutput = "New field " + FIELD1 + " created with value ''.*\n";
    expectedOutput += "Value '" + VALUE1 + "' written to field " + FIELD1 + " for GUID .*\n";
    expectedOutput += Pattern.quote("[\"" + VALUE1 + "\"]") + ".*\n";
    expectedOutput += "Field " + FIELD1 + " removed from GUID .*\n";
    expectedOutput += "New field " + FIELD1 + " created with value 'zero'.*\n";
    expectedOutput += Pattern.quote("[\"zero\"]") + ".*\n";
    expectedOutput += "Value 'one' appended to field " + FIELD1 + " for GUID .*\n";
    expectedOutput += Pattern.quote("[\"zero\",\"one\"]") + ".*\n";
    expectedOutput += "Value 'two' written at index 1 of field " + FIELD1 + " for GUID .*\n";
    expectedOutput += Pattern.quote("[\"zero\",\"two\"]") + ".*\n";
    expectedOutput += "Field " + FIELD1 + " cleared.*\n";
    expectedOutput += Pattern.quote("[]") + ".*\n";
    expectedOutput += "Field " + FIELD1 + " removed from GUID .*\n";
    expectedOutput += Pattern
            .quote("Failed to access GNS ( edu.umass.cs.gnsclient.exceptions.GnsException: General command failure: +GENERICEERROR+)")
            + ".*\n";
    expectedOutput += "Value '" + VALUE1 + "' written to field " + FIELD1 + " for GUID .*\n";
    expectedOutput += Pattern.quote("[\"" + VALUE1 + "\"]") + ".*\n";
    expectedOutput += "Value '" + VALUE2 + "' written to field " + FIELD1 + " for GUID .*\n";
    expectedOutput += Pattern.quote("[\"" + VALUE2 + "\"]") + ".*\n";
    expectedOutput += "Field " + FIELD1 + " removed from GUID .*\n";

    runCommandsInConsole(inCommands, expectedOutput, true, true);
  }

  /**
   * Test groups GUIDs and membership
   */
  @Test
  public void testGroupGuid() {
    String inCommands;
    String expectedOutput;

    // Create GUIDs
    inCommands = "guid_create " + GROUP_GUID + "\n";
    inCommands += "guid_create " + GUID1 + "\n";
    inCommands += "guid_create " + GUID2 + "\n";

    // Check and approve membership requestsa
    inCommands += "guid_use " + GROUP_GUID + "\n";
    inCommands += "group_member_list " + GROUP_GUID + "\n";
    inCommands += "group_member_add " + GUID1 + "\n";
    inCommands += "group_member_list\n";
    inCommands += "group_member_add " + GROUP_GUID + " " + GUID2 + "\n";
    inCommands += "group_member_list " + GROUP_GUID + "\n";

    // Remove a member
    inCommands += "group_member_remove " + GROUP_GUID + " " + GUID2 + "\n";
    inCommands += "group_member_list " + GROUP_GUID + "\n";

    // Cleanup GUIDs
    inCommands += "guid_delete " + GUID2 + "\n";
    inCommands += "guid_delete " + GUID1 + "\n";
    inCommands += "guid_delete " + GROUP_GUID + "\n";

    expectedOutput = "Looking for alias " + GROUP_GUID + " GUID and certificates...\n";
    expectedOutput += "Generating new GUID and keys for account .*\n";
    expectedOutput += "Created GUID .*\n";
    expectedOutput += "Looking for alias " + GUID1 + " GUID and certificates...\n";
    expectedOutput += "Generating new GUID and keys for account .*\n";
    expectedOutput += "Created GUID .*\n";
    expectedOutput += "Looking for alias " + GUID2 + " GUID and certificates...\n";
    expectedOutput += "Generating new GUID and keys for account .*\n";
    expectedOutput += "Created GUID .*\n";

    expectedOutput += "Looking up alias " + GROUP_GUID + " GUID and certificates...\n";
    expectedOutput += "Current GUID set to " + GROUP_GUID + ".*\n";
    expectedOutput += "Members in group .*\n";
    expectedOutput += "GUID " + GUID1 + " added to group .*\n";
    expectedOutput += "Members in group .*\n";
    expectedOutput += "0: " + GUID1 + ".*\n";
    expectedOutput += "GUID " + GUID2 + " added to group .*\n";
    expectedOutput += "Members in group .*\n";
    expectedOutput += "0: " + GUID1 + ".*\n";
    expectedOutput += "1: " + GUID2 + ".*\n";
    expectedOutput += "GUID " + GUID2 + " removed from group .*\n";
    expectedOutput += "Members in group .*\n";
    expectedOutput += "0: " + GUID1 + ".*\n";

    expectedOutput += "Looking up alias " + GUID2 + " certificates...\n";
    expectedOutput += "Alias " + GUID2 + " removed from GNS.\n";
    expectedOutput += "Keys for " + GUID2 + " removed from local repository..*\n";
    expectedOutput += "Looking up alias " + GUID1 + " certificates...\n";
    expectedOutput += "Alias " + GUID1 + " removed from GNS.\n";
    expectedOutput += "Keys for " + GUID1 + " removed from local repository..*\n";
    expectedOutput += "Looking up alias " + GROUP_GUID + " certificates...\n";
    expectedOutput += "Alias " + GROUP_GUID + " removed from GNS.\n";
    expectedOutput += "Keys for " + GROUP_GUID + " removed from local repository..*\n";

    runCommandsInConsole(inCommands, expectedOutput, true, true);
  }

  /**
   * Test GUID creation and deletion
   */
  @Test
  public void testGuidCreateDelete() {
    String inCommands;
    String expectedOutput;

    inCommands = "guid_create " + GUID1 + "\n";
    inCommands += "guid_delete " + GUID1 + "\n";

    expectedOutput = "Looking for alias " + GUID1 + " GUID and certificates...\n";
    expectedOutput += "Generating new GUID and keys for account .*\n";
    expectedOutput += "Created GUID .*\n";
    expectedOutput += "Looking up alias " + GUID1 + " certificates...\n";
    expectedOutput += "Alias " + GUID1 + " removed from GNS.\n";
    expectedOutput += "Keys for " + GUID1 + " removed from local repository..*\n";

    runCommandsInConsole(inCommands, expectedOutput, true, true);
  }

  /**
   * Test GUID creation and deletion 5 times
   */
  @Test
  public void test5GuidCreateDelete() {
    for (int i = 0; i < 5; i++) {
      testGuidCreateDelete();
    }
  }

  /**
   * Test account GUID creation and deletion
   */
  @Test
  public void testAccountGuidDelete() {
    String inCommands;
    String expectedOutput;

    inCommands = "account_delete " + ACCOUNT + "\n";

    expectedOutput = "Removed account GUID .*\n";

    runCommandsInConsole(inCommands, expectedOutput, true, false);
  }

  /**
   * Run inCommands commands through the CLI and compare the output with
   * expectedOutput. Can also check if a default GNS and/or GUID have been set.
   *
   * @param inCommands list of console commands separated by '\n'
   * @param expectedOutput list of expected console output (can use Java regular
   * expressions, each line separated by '\n')
   * @param requireDefaultGns check if a default GNS has been defined (will
   * error out if not)
   * @param requireDefaultAccountGuid check if a default GUID has been defined
   * (will error out if not)
   */
  protected void runCommandsInConsole(String inCommands, String expectedOutput, boolean requireDefaultGns,
          boolean requireDefaultAccountGuid) {
    boolean success = false;
    StringWriter output = new StringWriter();
    try {
      ConsoleReader consoleReader
              = new ConsoleReader(new ByteArrayInputStream(inCommands.getBytes("UTF-8")), output);
      ConsoleModule module = new ConsoleModule(consoleReader);
      module.printString("GNS Client Version: " + GNSClientConfig.readBuildVersion() + "\n");

      // Run the commands
      module.handlePrompt();

      // Check the output
      StringTokenizer expected = new StringTokenizer(expectedOutput, "\n");
      StringTokenizer actual = new StringTokenizer(output.toString(), "\n");

      if (!actual.hasMoreTokens()) {
        fail("No console output");
      }

      if (!actual.nextToken().startsWith("GNS Client Version")) {
        fail("Unexpected console output, should start with 'GNS Client Version'");
      }

      // Check that default GNS defaults is set
      if (requireDefaultGns) {
        if (!actual.hasMoreTokens()) {
          fail("Default GNS not set");
        }
        String defaultGns = actual.nextToken();
        if ("Default GNS: null".equals(defaultGns)) {
          fail("Default GNS not set");
        }
        if (!defaultGns.startsWith("Default GNS: ")) {
          fail("Unexpected console output, should start with 'Default GNS: '");
        }

        // Check GNS Connectivity .
        if (!actual.hasMoreTokens()) {
          fail("No console output during GNS connectivity check");
        }

        if (!actual.nextToken().startsWith("Checking GNS connectivity")) {
          fail("Unexpected console output during GNS connectivity check");
        }
        if (!actual.hasMoreTokens()) {
          fail("No console output during GNS connectivity check");
        }
        if (!actual.nextToken().startsWith("Connected to GNS")) {
          fail("Default GNS is not reachable");
        }
      } else { // Consume lines until we connected or not to a default GNS
        while (actual.hasMoreTokens()) {
          String line = actual.nextToken();
          if (line.startsWith("Connected to GNS")
                  || line.startsWith("Failed to connect to GNS")
                  || line.startsWith("Couldn't connect to default GNS")) {
            break;
          }
        }
      }

      if (requireDefaultAccountGuid) {
        // Check default GUID
        if (!actual.hasMoreTokens()) {
          fail("Default GUID not set");
        }
        String defaultGuid = actual.nextToken();
        if (defaultGuid.matches("Default GUID: null")) {
          fail("Default GUID not set");
        }
        if (!actual.hasMoreTokens()) {
          fail("Default GUID not set");
        }
        defaultGuid = actual.nextToken();
        if (!defaultGuid.matches("Looking up alias .*")) {
          fail("Unexpected console output, should start with 'Looking up alias'");
        }
        if (!actual.hasMoreTokens()) {
          fail("Default GUID not set");
        }
        if (!actual.nextToken().startsWith("Current GUID set to ")) {
          fail("Default GUID not set or not valid");
        }
        if (!actual.hasMoreTokens()) {
          fail("Default GUID not set or not valid");
        }

        // Next should be the console prompt
        if (!actual.nextToken().startsWith(GNS_CLI_PROMPT)) {
          fail("Default GUID not properly set, expected console prompt");
        }
      } else {
        // Consume all input until first prompt
        while (actual.hasMoreTokens() && !actual.nextToken().startsWith(GNS_CLI_PROMPT))
          ;
      }

      // Diff outputs
      while (expected.hasMoreTokens()) {
        String nextExpected = expected.nextToken();
        String nextActual = null;
        if (actual.hasMoreTokens()) { // Skip command line prompts to get real output
          nextActual = actual.nextToken();
          while (nextActual.startsWith(GNS_CLI_PROMPT) && actual.hasMoreTokens()) {
            nextActual = actual.nextToken();
          }
          // Treat expected as a regular expression
          if (!nextActual.matches("(?s)" + nextExpected)) {
            if (StringUtils.getLevenshteinDistance(nextActual, nextExpected) < 5) {
              for (int i = 0; i < nextActual.length(); i++) {
                final char actualChar = nextActual.charAt(i);
                if (i < expectedOutput.length()) {
                  final char expectedChar = nextExpected.charAt(i);
                  if (actualChar != expectedChar) {
                    System.out.println("Character " + i + " differs: " + ((int) actualChar) + " vs expected "
                            + ((int) expectedChar) + " - " + actualChar + "<>" + expectedChar + "\n");
                  }
                } else {
                  System.out.println("Character " + i + " is extra: " + ((int) actualChar) + " - " + actualChar);
                }
              }
            }
            fail("Got     : '" + nextActual + "'\n" + "Expected: '" + nextExpected + "'\n");
          }
        } else {
          fail("Missing expected output: " + nextExpected);
        }
      }

      // Check extra output
      while (actual.hasMoreTokens()) {
        String nextActual = actual.nextToken();
        while (nextActual.startsWith(GNS_CLI_PROMPT) && actual.hasMoreTokens()) {
          nextActual = actual.nextToken();
        }
        if (!nextActual.startsWith(GNS_CLI_PROMPT)) {
          fail("Extra output: " + actual.nextToken());
        }
      }

      success = true;
    } catch (IOException e) {
      fail("Error during console execution " + e);
    } finally {
      if (!success) {
        System.out.println("**** COMPLETE OUTPUT for " + testName.getMethodName() + " ****");
        System.out.println(output.toString());
      }
    }
  }
}
