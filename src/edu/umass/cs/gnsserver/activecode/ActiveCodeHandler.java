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
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

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

/**
 * This class is the entry of active code, it provides
 * the interface for GNS to run active code. 
 * It also checks whether it's necessary to run active code.
 * The conditions to run active code are:
 * 1. There is no internal field
 * 2. There exists a piece of active code on the corresponding action(e.g., read or wrote)
 * 3. Active code is enabled
 * 4. The value being passed into active code is not null
 * 5. It could not be a recovery process for paxos to roll forward its local DB
 *
 * @author Zhaoyu Gao, Westy
 */
public class ActiveCodeHandler {
	
  private static final Logger LOGGER = Logger.getLogger(ActiveCodeHandler.class.getName());
  
  /**
   *  This is used for DNS query to append source IP address to the value
   */
  public static final String SOURCE_IP_FIELD = "client_ip";
  
  /**
   * Debug level
   */
  public static final Level DEBUG_LEVEL = Level.FINE;

  private static ActiveHandler handler;

  private static String gigapaxoConfig = PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY;
  
  /**
   * This variable is used to check whether this operation is an op for paxos to roll
   * forward DB when system tries to recover from a failure.
   * True means the recovery has already finished.
   * False if the system is still under recovery.
   * 
   * The invariant here is:
   * active code should not be executed when system is under recovery.
   */
  private static boolean isFirstTimeWithDoNotReplyToClientFalse = false;
  
  /**
   * Initializes an ActiveCodeHandler
   *
   * @param nodeId
   */
  public ActiveCodeHandler(String nodeId) {
    
    String configFile = System.getProperty(gigapaxoConfig);
    if (configFile != null && new File(configFile).exists()) {
      try {
        new ActiveCodeConfig(configFile);
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }

    handler = new ActiveHandler(nodeId, new ActiveCodeDB(), ActiveCodeConfig.activeCodeWorkerCount, ActiveCodeConfig.activeWorkerThreads, ActiveCodeConfig.activeCodeBlockingEnabled);
  }


  /**
   * Check if the value contains an internal field
   */
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

  /**
   * @param header
   * @param code
   * @param guid
   * @param accessor
   * @param action
   * @param value
   * @param activeCodeTTL current default is 10
   * @return executed result
   * @throws InternalRequestException
   */
  private static JSONObject runCode(InternalRequestHeader header, String code, String guid, String accessor, 
          String action, JSONObject value, int activeCodeTTL) throws InternalRequestException {
    try {
      return handler.runCode(header, guid, accessor, code, value, activeCodeTTL);
    } catch (ActiveException e) {
      ActiveCodeHandler.getLogger().log(Level.INFO, "ActiveGNS request execution failed", e);
      /**
       * return the original value without executing, as there is an error
       * returned from the worker. The error indicates that the code failed
       * to execute on worker.
       * Note: cannot return null as specified by gigapaxos execute method
       */
      throw new InternalRequestException(ResponseCode.INTERNAL_REQUEST_EXCEPTION, "ActiveGNS request execution failed:" + e.getMessage());
    }
  }

  /**
   * This interface is used for the class out of activecode package to trigger active code.
   * It requires the parameters for running active code such as guid, field, and value.
   * It runs the requests and returns the processed result to the caller.
   *
   *
   * @param header header is needed for depth query
   * @param guid
   * @param field
   * @param action the actions in {@code ActiveCode}
   * @param value
   * @param db db is needed for fetching active code to run
   * @return the processed result as an JSONObject, the original value is returned if there is an error with code execution
   * @throws InternalRequestException
   */
  public static JSONObject handleActiveCode(InternalRequestHeader header,
          String guid, String field, String action, JSONObject value, BasicRecordMap db) 
          throws InternalRequestException {

    if (Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE) ) {
      return value;
    } 
    
    if(header != null){
    	// this is a depth query, and we do not call the code again, as it will form a infinite loop if not.
    	if(guid.equals(header.getOriginatingGUID()) && header.getTTL() < InternalRequestHeader.DEFAULT_TTL){    
    		return value;
    	}
    }else{
    	// without a header, the code can misbehave without any regulation, therefore we return the value immediately if no header presents
    	return value;
    }
    
    assert(!Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE));
    /**
     * check whether this is an operation for paxos to roll forward DB: MOB-1095
     * https://mobileinternet.atlassian.net/browse/MOB-1095
     * 
     * This variable needs to work together with the doNotReplyToClient variable
     * in the header. If isFirstTimeWithDoNotReplyToClientFalse is false, we keep
     * checking the value doNotReplyToClient in the header, until doNotReplyToClient
     * is false. When doNotReplyToClient turns to false, we set 
     * isFirstTimeWithDoNotReplyToClientFalse to true, and never need to check
     * doNotReplyToClient any more.  
     */
    if(!isFirstTimeWithDoNotReplyToClientFalse){
    	if(!header.getDoNotReplyToClient()){
    		isFirstTimeWithDoNotReplyToClientFalse = true;
    	}else{
    		// otherwise, do not run active code, return the original value directly
    		return value;
    	}
    }
    
    long t = System.nanoTime();
    ActiveCodeHandler.getLogger().log(DEBUG_LEVEL,
            "OOOOOOOOOOOOO ready to handle:[guid:{0},field:{1},action:{2},value:{3},header:{4}]",
            new Object[]{guid, field, action, value, header});
    /**
     * Only execute active code for user field
     * FIXME:
     * <p>
     * Read can be a single-field read or multi-field read.
     * If it's a single-field read, then the field can not be a internal field.
     * If it's a multi-field read, then there may be some field is internal.
     * <p>
     * Write has no field value, but if there should not be an internal
     * field in the JSONObject value.
     */
    if (
    		(action.equals(ActiveCode.READ_ACTION) && field != null && InternalField.isInternalField(field))
            || (action.equals(ActiveCode.WRITE_ACTION) && 
            		((value != null && containInternalField(value)) || (field !=null && InternalField.isInternalField(field))
            				))) {
    	ActiveCodeHandler.getLogger().log(DEBUG_LEVEL,
                "OOOOOOOOOOOOO no need to handle:[guid:{0},field:{1},action:{2},value:{3},header:{4}]",
                new Object[]{guid, field, action, value, header});
      return value;
    }
    JSONObject newResult = value;
    if (field == null || !InternalField.isInternalField(field)) {
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
        // Prepare values for query
        String accessorGuid = header == null ? guid : header.getOriginatingGUID();
        if(header.getSourceAddress() != null){
			try {
				value.put(SOURCE_IP_FIELD, header.getSourceAddress());
			} catch (JSONException e) {
				e.printStackTrace();
			}
        }
        // Run code
        newResult = runCode(header, code, guid, accessorGuid, action, value, header.getTTL());
        
        // Strip the appended fields
        if(newResult.has(SOURCE_IP_FIELD)){
        	newResult.remove(SOURCE_IP_FIELD);
        }
      }else if(codeMap == null){
    	  ActiveCodeHandler.getLogger().log(DEBUG_LEVEL,
                  "OOOOOOOOOOOOO no code to run:[guid:{0},field:{1},action:{2},value:{3},header:{4}]",
                  new Object[]{guid, field, action, value, header});
      }
    }
    ActiveCodeHandler.getLogger().log(DEBUG_LEVEL,
            "OOOOOOOOOOOOO The result after executing active code is {0}",
            new Object[]{newResult});
    DelayProfiler.updateDelayNano("activeTotal", t);
    return newResult;
  }

  /**
   * @return LOGGER
   */
  public static Logger getLogger() {
    return LOGGER;
  }

  /**
   * *************************** TEST CODE ********************
   */
  /**
   * @param args
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   * @throws JSONException
   * @throws InternalRequestException
   */
  public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, JSONException, InternalRequestException {
    new ActiveCodeHandler("Test");

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
