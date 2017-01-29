
package edu.umass.cs.gnsserver.activecode;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveHandler;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ActiveCodeHandler {

  private final String nodeId;

  private static final Logger LOGGER = Logger.getLogger(ActiveCodeHandler.class.getName());


  public static final Level DEBUG_LEVEL = Level.FINE;

  private static ActiveHandler handler;

  private static String gigapaxoConfig = PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY;


  public ActiveCodeHandler(String nodeId) {
    this.nodeId = nodeId;
    String configFile = System.getProperty(gigapaxoConfig);
    if (configFile != null && new File(configFile).exists()) {
      try {
        new ActiveCodeConfig(configFile);
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }

    handler = new ActiveHandler(nodeId, new ActiveCodeDB(), ActiveCodeConfig.activeCodeWorkerCount, ActiveCodeConfig.activeWorkerThreads, ActiveCodeConfig.acitveCodeBlockingEnabled);
  }


  private static boolean hasCode(ValuesMap valuesMap, String action) {

    try {
      if (valuesMap.get(ActiveCode.getCodeField(action)) != null) {
        return true;
      }
    } catch (JSONException | IllegalArgumentException e) {
      return false;
    }

    return false;
  }


  private static boolean containInternalField(JSONObject value) {
    boolean contained = false;
    Iterator<?> iter = value.keys();
    while (iter.hasNext()) {
      String field = (String) iter.next();
      if (InternalField.isInternalField(field)) {
        return true;
      }
    }
    return contained;
  }


  private static JSONObject runCode(InternalRequestHeader header, String code, String guid, String accessor, 
          String action, JSONObject value, int activeCodeTTL) throws InternalRequestException {
    try {
      return handler.runCode(header, guid, accessor, code, value, activeCodeTTL);
    } catch (ActiveException e) {
      ActiveCodeHandler.getLogger().log(Level.INFO, "ActiveGNS request execution failed", e);

      throw new InternalRequestException(ResponseCode.INTERNAL_REQUEST_EXCEPTION, "ActiveGNS request execution failed:" + e.getMessage());
    }
  }


  public static JSONObject handleActiveCode(InternalRequestHeader header,
          String guid, String field, String action, JSONObject value, BasicRecordMap db) 
          throws InternalRequestException {

    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
      return value;
    }

    long t = System.nanoTime();
    ActiveCodeHandler.getLogger().log(DEBUG_LEVEL,
            "OOOOOOOOOOOOO handles:[guid:{0},field:{1},action:{2},value:{3},header:{4}]",
            new Object[]{guid, field, action, value, header});

    if (action.equals(ActiveCode.READ_ACTION) && field != null && InternalField.isInternalField(field)
            || (action.equals(ActiveCode.WRITE_ACTION) && value != null && containInternalField(value))) {
      return value;
    }
    JSONObject newResult = value;
    if (field == null || !InternalField.isInternalField(field)) {
      //FIXME: Seems like this field lookup all could be replaced by something 
      // like NSFieldAccess.lookupJSONFieldLocalNoAuth
      NameRecord activeCodeNameRecord = null;
      try {
        activeCodeNameRecord = NameRecord.getNameRecordMultiUserFields(db, guid,
                ColumnFieldType.USER_JSON, ActiveCode.getCodeField(action));
      } catch (RecordNotFoundException | FailedDBOperationException | IllegalArgumentException e) {
        e.printStackTrace();
        return value;
      }

      ValuesMap codeMap = null;
      try {
        codeMap = activeCodeNameRecord.getValuesMap();
      } catch (FieldNotFoundException e) {
        e.printStackTrace();
        return value;
      }

      if (codeMap != null && value != null) {
        String code;
        try {
          code = codeMap.getString(ActiveCode.getCodeField(action));
        } catch (JSONException | IllegalArgumentException e) {
          return value;
        }
        String accessorGuid = header == null ? guid : header.getOriginatingGUID();
        newResult = runCode(header, code, guid, accessorGuid, action, value, 5);
      }
    }
    ActiveCodeHandler.getLogger().log(DEBUG_LEVEL,
            "OOOOOOOOOOOOO The result after executing active code is {0}",
            new Object[]{newResult});
    DelayProfiler.updateDelayNano("activeTotal", t);
    return newResult;
  }


  public static Logger getLogger() {
    return LOGGER;
  }



  public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, JSONException, InternalRequestException {
    ActiveCodeHandler handler = new ActiveCodeHandler("Test");

    // initialize the parameters used in the test 
    JSONObject obj = new JSONObject();
    obj.put("testGuid", "success");
    ValuesMap valuesMap = new ValuesMap(obj);
    final String guid1 = "guid";
    final String field1 = "testGuid";
    final String read_action = "read";

    String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
    String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
    ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);

    int n = 1000000;
    long t = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);
    }
    long elapsed = System.currentTimeMillis() - t;
    System.out.println(String.format("it takes %d ms, avg_latency = %f us", elapsed, elapsed * 1000.0 / n));

  }

}
