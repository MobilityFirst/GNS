package edu.umass.cs.gnsclient.client.singletests;

import java.nio.file.Files;
import java.nio.file.Paths;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import java.io.IOException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import edu.umass.cs.utils.Utils;
import org.json.JSONObject;
import org.junit.Assert;

/**
 * Active Code Tests
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ActiveCodeTest {

  private static final String ACCOUNT_ALIAS = "support@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;

  private static final String FIELD = "someField";
  private static final String ORIGINAL_VALUE = "original value";

  private static final String OTHER_FIELD = "otherField";
  private static final String OTHER_RESULT = "other field succeeds";

  public ActiveCodeTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
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
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);

    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_020_ActiveCodeUpdateFields() {
    try {
      // set up a field
      client.fieldUpdate(masterGuid, FIELD, ORIGINAL_VALUE);
      client.fieldUpdate(masterGuid, OTHER_FIELD, OTHER_RESULT);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_030_ActiveCodeClearCode() {
    try {
      // clear code for both read and write action
      client.activeCodeClear(masterGuid.getGuid(), ActiveCode.READ_ACTION, masterGuid);
      client.activeCodeClear(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, masterGuid);
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
    try {
      // get the value of the field
      String actual = client.fieldRead(masterGuid, FIELD);
      Assert.assertEquals(ORIGINAL_VALUE, actual);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_050_ActiveCodeSetReadCode() {
    String readcode = null;
    String writecode = null;
    try {
      // read in the code as a string
      readcode = new String(Files.readAllBytes(Paths.get("scripts/activeCode/testing/readTest.js")));
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception reading code file: " + e);
    }
    try {
      // set up the code for on read operation
      client.activeCodeSet(masterGuid.getGuid(), ActiveCode.READ_ACTION, readcode, masterGuid);
      //client.activeCodeSet(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, writecode, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Setting active code: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_060_ActiveCodeCheckModified() {
    try {
      // get the value of the field again
      String actual = client.fieldRead(masterGuid, FIELD);
      Assert.assertEquals("updated value", actual);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_070_ActiveCodeCheckOther() {
    try {
      // make sure the other field still works
      String actual = client.fieldRead(masterGuid, OTHER_FIELD);
      Assert.assertEquals(OTHER_RESULT, actual);
    } catch (Exception e) {
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
    String writecode = null;
    try {
      // read in the code as a string
      writecode = new String(Files.readAllBytes(Paths.get("scripts/activeCode/testing/writeTest.js")));
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception reading code file: " + e);
    }
    try {
      // set up the code for on read operation
      client.activeCodeSet(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, writecode, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Setting active code: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_090_ActiveCodeUpdate() {
    try {
      // get the value of the field again
      client.update(masterGuid, new JSONObject("{\"test1\":\"value1\"}"));
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_100_ActiveCodeCheckModifiedWrite() {
    try {
      // get the value of the field again
      String actual = client.fieldRead(masterGuid, "test1");
      Assert.assertEquals("updated value1", actual);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_110_ActiveCodeClearCode() {
    try {
      // clear code for both read and write action
      client.activeCodeClear(masterGuid.getGuid(), ActiveCode.READ_ACTION, masterGuid);
      client.activeCodeClear(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, masterGuid);
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
    String writecode = null;
    try {
      // read in the code as a string
      writecode = new String(Files.readAllBytes(Paths.get("scripts/activeCode/testing/writeFromReadGuidTest.js")));
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception reading code file: " + e);
    }
    try {
      // set up the code for on read operation
      client.activeCodeSet(masterGuid.getGuid(), ActiveCode.WRITE_ACTION, writecode, masterGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Setting active code: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_130_ActiveCodeUpdateAgain() {
    try {
      // get the value of the field again
      client.update(masterGuid, new JSONObject("{\"test2\":\"value2\"}"));
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_140_ActiveCodeCheckModifiedWriteFromRead() {
    try {
      // get the value of the field again
      System.out.println(client.read(masterGuid));
      String actual = client.fieldRead(masterGuid, "test1");
      Assert.assertEquals("original value", actual);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }
}
