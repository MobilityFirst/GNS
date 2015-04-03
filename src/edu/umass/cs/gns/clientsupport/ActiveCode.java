package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;

public class ActiveCode {
	/**
	 * Active code fields
	 */
	public static final String ON_READ = InternalField.makeInternalFieldString("on_read");
	public static final String ON_WRITE = InternalField.makeInternalFieldString("on_write");

	/**
	 * Returns the internal field corresponding to the given action
	 * @param action
	 * @return
	 */
	public static String codeField(String action) {
		if(action.equals("read")) {
			return ON_READ;
		}
		else if(action.equals("write")) {
			return ON_WRITE;
		}
		else {
			return null;
		}
	}
	
	/**
	 * Initializes the fields (called upon guid creation) to prevent undefined behavior
	 * @param guid
	 * @param handler
	 */
	public static void initCodeFields(String guid, 
			ClientRequestHandlerInterface handler) {
		String empty = null;
		handler.getIntercessor().sendUpdateRecordBypassingAuthentication(guid, ON_READ, empty, null, 
				UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE);
		handler.getIntercessor().sendUpdateRecordBypassingAuthentication(guid, ON_WRITE, empty, null, 
				UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE);
	}

	/**
	 * Sets active code for the guid and action
	 * @param guid
	 * @param action
	 * @param code
	 * @param writer
	 * @param signature
	 * @param message
	 * @param handler
	 * @return
	 */
	public static NSResponseCode setCode(String guid, String action, String code, String writer, String signature, String message,
			ClientRequestHandlerInterface handler) {
		String field = codeField(action);
		NSResponseCode response = handler.getIntercessor().sendUpdateRecord(guid, field, code, null, 1,
				UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, writer, signature, message);
		return response;
	}

	/**
	 * Clears the active code for the guid and action
	 * @param guid
	 * @param action
	 * @param code
	 * @param writer
	 * @param signature
	 * @param message
	 * @param handler
	 * @return
	 */
	public static NSResponseCode clearCode(String guid, String action, String writer, String signature, String message,
			ClientRequestHandlerInterface handler) {
		String field = codeField(action);
		String clear = null;
		NSResponseCode response = handler.getIntercessor().sendUpdateRecord(guid, field, clear, null, 0,
				UpdateOperation.SINGLE_FIELD_CLEAR, writer, signature, message);
		return response;
	}


	/**
	 * Gets the currently set active code for the guid and action
	 * @param guid
	 * @param action
	 * @param code
	 * @param writer
	 * @param signature
	 * @param message
	 * @param handler
	 * @return
	 */
	public static ResultValue getCode(String guid, String action, String reader, String signature, String message,
			ClientRequestHandlerInterface handler) {
		String field = codeField(action);
		QueryResult result = handler.getIntercessor().sendQuery(guid, field, reader, signature, message, ColumnFieldType.LIST_STRING);
		if (!result.isError()) {
			return new ResultValue(result.getArray(field));
		} else {
			return new ResultValue();
		}
	}
}
