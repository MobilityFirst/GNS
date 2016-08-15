package edu.umass.cs.gnsserver.activecode;

import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.activegns.ActiveGNSClient;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 *
 */
public class ActiveCodeDB implements ActiveDBInterface {
	
	GNSApplicationInterface<?> gnsApp;
	ActiveGNSClient client;
	
	/**
	 * @param gnsApp
	 */
	public ActiveCodeDB(GNSApplicationInterface<?> gnsApp){
		this.gnsApp = gnsApp;
		try {
			this.client = new ActiveGNSClient();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID, String field) throws ClientException {
		return client.read(header, targetGUID, field);
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID, ArrayList<String> fields)
			throws ClientException {
		return client.read(header, targetGUID, fields);
	}

	@Override
	public void write(InternalRequestHeader header, String targetGUID, String field, JSONObject valuesMap)
			throws ClientException {
			client.write(header, targetGUID, field, valuesMap);		
	}

	
	private boolean writeSomeGuidToLocal(String guid, String field, ValuesMap value){
		try {
			NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(gnsApp.getDB(), 
					guid, ColumnFieldType.USER_JSON, field);
											
			nameRecord.updateNameRecord(field, null, null, 0, value,
			         UpdateOperation.USER_JSON_REPLACE_OR_CREATE);
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private ValuesMap readSomeGuidFromLocal(String guid, String field){
		ValuesMap value = null;
		try {
			NameRecord record = NameRecord.getNameRecordMultiUserFields(gnsApp.getDB(), guid, ColumnFieldType.USER_JSON, field);
			value = record.getValuesMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
	
	/**
	 * The remote read doesn't mean the guid must have to be on a remote GNS, it just uses
	 * a GNSClient to send a query a GNS which holds the record of the guid.
	 * 
	 * @param querierGuid
	 * @param queriedGuid
	 * @param field
	 * @return
	 */
	private ValuesMap readSomeFieldFromRemote(String querierGuid, String queriedGuid, String field) {
		ValuesMap value = null;
		/**
		 *  Do a common read here, follows the flow:
		 *  <p> server side: GNSCommand.fieldRead(String targetGuid, String field, GuidEntry guid);
		 *  <p> client side: GNSClientCommands.fieldRead -> CommandUtils.specialCaseSingleField -> 
		 *  GNSClientCommands.getResponse -> CommandUtils.checkResponse -> 
		 */
		try{
			String response = CommandUtils.checkResponse(client.sendCommandAndWait( CommandUtils.createCommand(CommandType.Read, 
							GNSCommandProtocol.GUID, queriedGuid, 
							GNSCommandProtocol.FIELD, field, 
							GNSCommandProtocol.READER, GNSConfig.GNSC.INTERNAL_OP_SECRET)));

			System.out.println(this+" receives response from remote GNS "+response);
			value = new ValuesMap(new JSONObject(response));
		} catch (Exception e){
			e.printStackTrace();
		}
		
		return value;
	}
	
	
}
