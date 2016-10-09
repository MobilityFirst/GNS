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

import java.util.concurrent.ConcurrentMap;


/**
 * This class defines a MSocketGroupWriter
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ContextWriter
{
	//public static double CHECK_RADIUS = 75;
	private final String writerName;
	private ContextWriterInternals msocketGroupWriterInternalsObj   = null;
	
	/**
	 * @param groupName: multicast groupName
	 * @throws Exception
	 */
	public ContextWriter(String writerName, String groupQuery) throws Exception
	{
		this.writerName = writerName;
		msocketGroupWriterInternalsObj = new ContextWriterInternals(writerName, groupQuery);
	}
	
	/**
	 * color:red type select call
	 * @param groupName
	 * @throws Exception
	 */
	/*public void connect() throws Exception 
	{
		//msocketGroupWriterInternalsObj.setGroupName(groupName);
		msocketGroupWriterInternalsObj.createGroup();
		//msocketGroupWriterInternalsObj.startGroupMaintainThread();
	}*/
	
	/*public InputStream getInputStream()
	{
		if(mcIn == null)
		{
			mcIn = new ContextWriterInputStream(msocketGroupWriterInternalsObj);
		}
		return mcIn;
	}*/
	
	/*public OutputStream getOutputStream()
	{
		if(mcOut == null)
		{
			mcOut = new ContextWriterOutputStream(msocketGroupWriterInternalsObj);
		}
		return mcOut;
	}*/
	
	/**
	 * Returns a concurrent map that keeps dynamically changing in the
	 * background.
	 * @return
	 */
	public ConcurrentMap<String, ContextSocket> accept()
	{
		return msocketGroupWriterInternalsObj.getConnectionMap();
	}
	
	public void writeAll(byte[] byteArray, int offset, int length)
	{
		msocketGroupWriterInternalsObj.writeAll(byteArray, offset, length);
	}
	
	public void close()
	{
		msocketGroupWriterInternalsObj.close();
	}
	
	/*public void printGroupMembers()
	{
		msocketGroupWriterInternalsObj.printGroupMembers();
	}*/
	
	/**
	 * In msecs
	 * @param start
	 */
	/*public void setGroupUpdateDelay(int delay)
	{
		msocketGroupWriterInternalsObj.setGroupUpdateDelay(delay);
	}
	
	public long getGroupUpdateDelay()
	{
		return msocketGroupWriterInternalsObj.getGroupUpdateDelay();
	}*/
}