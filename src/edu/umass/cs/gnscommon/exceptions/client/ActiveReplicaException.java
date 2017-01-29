
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class ActiveReplicaException extends ClientException
{
  private static final long serialVersionUID = 2676899572105162853L;
  

	public ActiveReplicaException(ResponseCode code, String message) {
		super(code, message);
	}


	public ActiveReplicaException(String message) {
		super(ResponseCode.ACTIVE_REPLICA_EXCEPTION, message);
	}
}
