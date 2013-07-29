package edu.umass.cs.gnrs.packet;

/*************************************************************
 * This class describes the record types, class and error 
 * codes for the resource record packet.
 * @author Hardeep Uppal
 *
 ************************************************************/
public class DNSRecordType {
	
	/** No error condition in response **/
	public final static int RCODE_NO_ERROR = 0;

	/** Response Error **/
	public final static int RCODE_ERROR = 1;
	
	public final static int RCODE_ERROR_INVALID_ACTIVE_NAMESERVER = 2;
	
	/** Query **/
	public final static int QUERY = 0;
	
	/** Response **/
	public final static int RESPONSE = 1;
}
