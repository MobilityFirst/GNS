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

import java.util.concurrent.ThreadFactory;

/**
 * This ThreadFactory allocates an ActiveCodeClient for every new worker thread.
 * @author mbadov
 *
 */
public class ActiveCodeThreadFactory implements ThreadFactory {
	ClientPool clientPool;
	
	public ActiveCodeThreadFactory(ClientPool clientPool) {
		this.clientPool = clientPool;
	}
	
	/**
	 * Creates a new thread and also spawns a new worker associated with the thread
	 */
        @Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		clientPool.addClient(t);
	    return t;
	}
}
