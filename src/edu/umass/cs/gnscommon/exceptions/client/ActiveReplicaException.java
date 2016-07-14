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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.GNSResponseCode;

/**
 * This class defines a ActiveReplicaException
 * 
 * @version 1.0
 */
public class ActiveReplicaException extends ClientException
{
  private static final long serialVersionUID = 2676899572105162853L;
  
	/**
	 * @param code
	 * @param message
	 */
	public ActiveReplicaException(GNSResponseCode code, String message) {
		super(code, message);
	}

	/**
	 * @param message
	 */
	public ActiveReplicaException(String message) {
		super(GNSResponseCode.ACTIVE_REPLICA_EXCEPTION, message);
	}
}
