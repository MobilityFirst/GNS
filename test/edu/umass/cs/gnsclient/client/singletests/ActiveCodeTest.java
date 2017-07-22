package edu.umass.cs.gnsclient.client.singletests;

import java.nio.file.Files;
import java.nio.file.Paths;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Config;
import java.io.IOException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import edu.umass.cs.utils.Utils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;

/**
 * Active Code Tests
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ActiveCodeTest extends DefaultGNSTest {
  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;

  private static final String FIELD = "someField";
  private static final String ORIGINAL_VALUE = "original value";

  private static final String OTHER_FIELD = "otherField";
  private static final String OTHER_RESULT = "other field succeeds";

  /**
   *
   */
  public ActiveCodeTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_010_ActiveCodeCreateGuids() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_020_ActiveCodeUpdateFields() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // set up a field
      clientCommands.fieldUpdate(masterGuid, FIELD, ORIGINAL_VALUE);
      clientCommands.fieldUpdate(masterGuid, OTHER_FIELD, OTHER_RESULT);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_030_ActiveCodeClearCode() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // clear code for both read and write action
      clientCommands.activeCodeClear(masterGuid.getGuid(), ActiveCode.READ_ACTION, masterGuid);
      clientCommands.activeCodeClear(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  //
  // Active code read test - see also the script mentioned below
  //
  /**
   *
   */
  @Test
  public void test_040_ActiveCodeCheckUnmodified() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // get the value of the field
      String actual = clientCommands.fieldRead(masterGuid, FIELD);
      Assert.assertEquals(ORIGINAL_VALUE, actual);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_050_ActiveCodeSetReadCode() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    String readcode = null;
    try {
      // read in the code as a string
      readcode = new String(Files.readAllBytes(Paths.get("scripts/activeCode/testing/readTest.js")));
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception reading code file: " + e);
    }
    try {
      // set up the code for on read operation
      clientCommands.activeCodeSet(masterGuid.getGuid(), ActiveCode.READ_ACTION, readcode, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Setting active code: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_060_ActiveCodeCheckModified() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // get the value of the field again
      String actual = clientCommands.fieldRead(masterGuid, FIELD);
      Assert.assertEquals("updated value", actual);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_070_ActiveCodeCheckOther() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // make sure the other field still works
      String actual = clientCommands.fieldRead(masterGuid, OTHER_FIELD);
      Assert.assertEquals(OTHER_RESULT, actual);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  //
  //
  // Another active code write test - see also the script mentioned below
  //
  /**
   *
   */
  @Test
  public void test_080_ActiveCodeSetWriteCode() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    String writecode = null;
    try {
      // read in the code as a string
      writecode = new String(Files.readAllBytes(Paths.get("scripts/activeCode/testing/writeTest.js")));
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception reading code file: " + e);
    }
    try {
      // set up the code for on read operation
      clientCommands.activeCodeSet(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, writecode, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Setting active code: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_090_ActiveCodeUpdate() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // get the value of the field again
      clientCommands.update(masterGuid, new JSONObject("{\"test1\":\"value1\"}"));
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_100_ActiveCodeCheckModifiedWrite() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // get the value of the field again
      String actual = clientCommands.fieldRead(masterGuid, "test1");
      Assert.assertEquals("updated value1", actual);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_110_ActiveCodeClearCode() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // clear code for both read and write action
      clientCommands.activeCodeClear(masterGuid.getGuid(), ActiveCode.READ_ACTION, masterGuid);
      clientCommands.activeCodeClear(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  //
  //
  // Another active code write test - see also the script mentioned below
  //
  /**
   *
   */
  @Test
  public void test_120_ActiveCodeSetWriteFromReadCode() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    String writecode = null;
    try {
      // read in the code as a string
      writecode = new String(Files.readAllBytes(Paths.get("scripts/activeCode/testing/writeFromReadGuidTest.js")));
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception reading code file: " + e);
    }
    try {
      // set up the code for on read operation
      clientCommands.activeCodeSet(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, writecode, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Setting active code: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_130_ActiveCodeUpdateAgain() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // get the value of the field again
      clientCommands.update(masterGuid, new JSONObject("{\"test2\":\"value2\"}"));
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_140_ActiveCodeCheckModifiedWriteFromRead() {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      System.out.println("Active code is disabled!");
      return;
    }
    try {
      // get the value of the field again
      System.out.println(clientCommands.read(masterGuid));
      String actual = clientCommands.fieldRead(masterGuid, "test1");
      Assert.assertEquals("original value", actual);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }
  
  /**
  *
  */
 @Test
 public void test_150_ActiveCodeClearCodeAgain() {
   if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
     System.out.println("Active code is disabled!");
     return;
   }
   try {
	 // Don't forget to clean up the code again
     // clear code for both read and write action
     clientCommands.activeCodeClear(masterGuid.getGuid(), ActiveCode.READ_ACTION, masterGuid);
     clientCommands.activeCodeClear(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, masterGuid);
   } catch (ClientException | IOException e) {
     Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
   }
 }
}
