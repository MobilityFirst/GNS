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

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ValuesMap;

public class ActiveCodeQueryHelper {
	private static final ArrayList<ColumnField> empty = new ArrayList<ColumnField>();
	private GnsApplicationInterface app;
	
	public ActiveCodeQueryHelper(GnsApplicationInterface app) {
		this.app = app;
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
		// Do a local read/write
		if(acqreq.getGuid() == null || acqreq.getGuid().equals(currentGuid)) {
			if(acqreq.getAction().equals("read")) {
				return readLocalGuid(currentGuid, acqreq.getField());
			} else if(acqreq.getAction().equals("write")) {
				return writeLocalGuid(currentGuid, acqreq.getField(), acqreq.getValuesMapString());
			}
		}
		// Otherwise, we need to do an external read
		else {
			if(acqreq.getAction().equals("read")) {
				// TODO
			}
		}
		
		// Return failure
		return new ActiveCodeQueryResponse();
	}
}
