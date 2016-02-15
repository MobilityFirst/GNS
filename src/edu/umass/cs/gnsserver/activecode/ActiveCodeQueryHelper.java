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

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is used for handling queries from
 * active code worker and interact with local DB.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeQueryHelper {
	private GnsApplicationInterface<String> app;
	private ActiveCodeHandler ach;
	
	/**
	 * Initialize an ActiveCodeQueryHelper
	 * @param app
	 * @param ach 
	 */
	public ActiveCodeQueryHelper(GnsApplicationInterface<String> app, ActiveCodeHandler ach) {
		this.app = app;
		this.ach = ach;
	}

	/**
	 * Reads a local guid/field from the GNS
	 * @param guid the guid
	 * @param field the field
	 * @return the ValuesMap object encapsulated in a ActiveCodeQueryResponse object
	 */
	private ActiveCodeQueryResponse readLocalGuid(String guid, String field) {
		String valuesMapString = null;
		boolean success = false;
		
		try {
			NameRecord nameRecord = NameRecord.getNameRecordMultiField(app.getDB(), guid, null, ColumnFieldType.USER_JSON, field);
			if(nameRecord.containsKey(field)) {
				ValuesMap vm = nameRecord.getValuesMap();
				valuesMapString = vm.toString();
				success = true;
			}
		} catch (RecordNotFoundException | FailedDBOperationException | FieldNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new ActiveCodeQueryResponse(success, valuesMapString);
	}
	
	/**
	 * Writes a values map to a field for a given local guid
	 * @param guid the guid
	 * @param field the field
	 * @param valuesMapString the values map object
	 * @return an ActiveCodeQueryResponse object indicating the status of the write
	 */
	private ActiveCodeQueryResponse writeLocalGuid(String guid, String field, String valuesMapString) {
		boolean success = false;
		
		try {
			ValuesMap userJSON = new ValuesMap(new JSONObject(valuesMapString));
			NameRecord nameRecord = NameRecord.getNameRecordMultiField(app.getDB(), guid, null, ColumnFieldType.USER_JSON, field);
			nameRecord.updateNameRecord(field, null, null, 0, userJSON,
		              UpdateOperation.USER_JSON_REPLACE_OR_CREATE);
			success = true;
		} catch (RecordNotFoundException | FailedDBOperationException | FieldNotFoundException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new ActiveCodeQueryResponse(success, null);
	}

	/**
	 * Handles query requests from the child active code worker processes
	 * @param currentGuid the guid
	 * @param acqreq the query request object
	 * @return the response, which may contain values read, or just status for a write
	 */
	public ActiveCodeQueryResponse handleQuery(String currentGuid, ActiveCodeQueryRequest acqreq) {
		// Do a local read/write for the same guid without triggering the active code
		String targetGuid = acqreq.getGuid();
		String field = acqreq.getField();
		int hopLimit = acqreq.getLimit();
		ActiveCodeQueryResponse acqr = null;
		
		if(targetGuid == null || targetGuid.equals(currentGuid)) {
			if(acqreq.getAction().equals("read")) {
				acqr = readLocalGuid(currentGuid, field);
			} else if(acqreq.getAction().equals("write")) {
				acqr =  writeLocalGuid(currentGuid, field, acqreq.getValuesMapString());
			}
		}
		// Otherwise, we need to do an external guid read
		else {
			//System.out.println("Got the query from guid "+currentGuid+" to access the field "+field+" of guid "+targetGuid);
			if(acqreq.getAction().equals("read")) {
				try{
					long t1 = System.nanoTime();
					boolean allowAccess = false;
					String publicKey = NSAuthentication.lookupPublicKeyInAcl(currentGuid, field, targetGuid, 
							MetaDataTypeName.READ_WHITELIST, app, ach.getAddress());
					DelayProfiler.updateDelayNano("activeCodeWhiteListVerification", t1);
					if (publicKey != null){
						allowAccess = true;
					}
					if (allowAccess){
						long start = System.nanoTime();
						//Read the record and code from DB
						NameRecord nameRecord = NameRecord.getNameRecordMultiField(app.getDB(), targetGuid, null, 
								  ColumnFieldType.USER_JSON, field);
						NameRecord codeRecord = NameRecord.getNameRecordMultiField(app.getDB(), targetGuid, null,
				                  ColumnFieldType.USER_JSON, ActiveCode.ON_READ);
						//set the response value to the code before running the active code
						acqr = new ActiveCodeQueryResponse(true, nameRecord.getValuesMap().toString());
						DelayProfiler.updateDelayNano("activeCodeCheckDBForRecord", start);
						start = System.nanoTime();
						if (codeRecord != null && nameRecord != null && ach.hasCode(codeRecord, "read")) {
							String code64 = codeRecord.getValuesMap().getString(ActiveCode.ON_READ);
							ValuesMap originalValues = nameRecord.getValuesMap();
							ValuesMap newResult = null;
							//System.out.println("Ready to do this external query for "+targetGuid+" on field "+field+" with the original value "+originalValues.toString());
							newResult = ach.runCode(code64, targetGuid, field, "read", originalValues, hopLimit);
							
							if (newResult != null){
								acqr = new ActiveCodeQueryResponse(true, newResult.toString());
								//System.out.println("Send the request with new result value "+newResult.toString());
							} else {
								acqr = new ActiveCodeQueryResponse(true, nameRecord.toString());
								//System.out.println("Send the request with new record value "+nameRecord.toString());
							}
						}
						DelayProfiler.updateDelayNano("activeCodeQuerierReadExecution", start);
					}else{
						//TODO: terminate the execution of the active code and return
						acqr = new ActiveCodeQueryResponse(false, null);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
				
			} else if(acqreq.getAction().equals("write")){
				//TODO: implement write operation for external guid write
				try{
					long t1 = System.nanoTime();
					boolean allowAccess = false;
					String publicKey = NSAuthentication.lookupPublicKeyInAcl(currentGuid, field, targetGuid, 
							MetaDataTypeName.WRITE_WHITELIST, app, ach.getAddress());
					DelayProfiler.updateDelayNano("activeCodeWhiteListVerification", t1);
					
					if (publicKey != null){
						allowAccess = true;
					}
					if (allowAccess){
						long start = System.nanoTime();
						NameRecord codeRecord = NameRecord.getNameRecordMultiField(app.getDB(), targetGuid, null,
				                  ColumnFieldType.USER_JSON, ActiveCode.ON_WRITE);
						DelayProfiler.updateDelayNano("activeCodeCheckDBForRecord", start);
						start = System.nanoTime();
						if (codeRecord != null && ach.hasCode(codeRecord, "write")) {
							String code64 = codeRecord.getValuesMap().getString(ActiveCode.ON_WRITE);
							ValuesMap userJSON = new ValuesMap(new JSONObject(acqreq.getValuesMapString()));
							NameRecord nameRecord = NameRecord.getNameRecordMultiField(app.getDB(), targetGuid, null, ColumnFieldType.USER_JSON, field);
							nameRecord.updateNameRecord(field, null, null, 0, userJSON,
						              UpdateOperation.USER_JSON_REPLACE_OR_CREATE);
							ach.runCode(code64, targetGuid, field, "write", nameRecord.getValuesMap(), hopLimit);
							acqr = new ActiveCodeQueryResponse(true, null);
						}
						DelayProfiler.updateDelayNano("activeCodeQuerierWriteExecution", start);
					}else{
						// TODO: terminate the execution of the active code and return
						acqr = new ActiveCodeQueryResponse(false, null);
						
					}
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		
		return acqr;
	}
}
