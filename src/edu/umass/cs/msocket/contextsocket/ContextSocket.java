/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.contextsocket;

import java.io.InputStream;
import java.io.OutputStream;

import edu.umass.cs.msocket.MSocket;

public class ContextSocket
{
	private ContextSocketInputStream  mcIn   	=  null;
	private ContextSocketOutputStream mcOut  	= null;
	
	private MSocket accptMSocket;
	
	public ContextSocket(MSocket accptMSocket)
	{
		this.accptMSocket = accptMSocket;
	}
	
	public InputStream getInputStream()
	{
		if(mcIn == null)
		{
			mcIn = new ContextSocketInputStream(accptMSocket);
		}
		return mcIn;
	}
	
	public OutputStream getOutputStream()
	{
		if(mcOut == null)
		{
			mcOut = new ContextSocketOutputStream(accptMSocket);
		}
		return mcOut;
	}
}