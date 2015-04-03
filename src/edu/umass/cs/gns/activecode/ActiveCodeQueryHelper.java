package edu.umass.cs.gns.activecode;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.nsdesign.clientsupport.NSFieldAccess;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeQueryHelper {
	private static final ArrayList<ColumnField> empty = new ArrayList<ColumnField>();
	private GnsApplicationInterface app;
	
	public ActiveCodeQueryHelper(GnsApplicationInterface app) {
		this.app = app;
	}

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

	public ActiveCodeQueryResponse handleQuery(String currentGuid, ActiveCodeQueryRequest acqreq) {		
		// Do a local read/write
		if(acqreq.guid == null || acqreq.guid.equals(currentGuid)) {
			if(acqreq.action.equals("read")) {
				return readLocalGuid(currentGuid, acqreq.field);
			} else if(acqreq.action.equals("write")) {
				return writeLocalGuid(currentGuid, acqreq.field, acqreq.valuesMapString);
			}
		}
		// Otherwise, we need to do an external read
		else {
			if(acqreq.action.equals("read")) {
				
			}
		}
		
		// Return failure
		return new ActiveCodeQueryResponse();
	}
}
