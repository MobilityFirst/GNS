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

package edu.umass.cs.msocket.apps;

import java.util.HashMap;
import java.util.Map;

import edu.umass.cs.msocket.contextsocket.ContextMember;
import edu.umass.cs.msocket.contextsocket.ContextSocket;
import edu.umass.cs.msocket.contextsocket.ContextWriter;


public class BothMember2WriterExp
{
	private ContextMember groupMember1;
	private ContextMember groupMember2;
	private ContextWriter groupWriter;
	
	public static void main(String[] args) throws InterruptedException
	{
		String memberName1 = args[0];
		String memberName2 = args[1];
		String writerName  = args[2];
		
		BothMember2WriterExp obj = new BothMember2WriterExp();
		obj.startProcess(memberName1, memberName2, writerName);
	}
	
	public void startProcess(String memberName1, String memberName2, String writerName) throws InterruptedException
	{
		GroupMemberThread memObj1  = new GroupMemberThread(memberName1, 1);
		GroupMemberThread memObj2  = new GroupMemberThread(memberName2, 2);
		GroupWriterThread writeObj = new GroupWriterThread(writerName);
		new Thread(memObj1).start();
		new Thread(memObj2).start();
		
		
		Thread.sleep(5000);
		new Thread(writeObj).start();
		
		Thread.sleep(20000);
		System.out.println("attr Update Start "+System.currentTimeMillis());
		groupMember2.setAttributes("contextATT0", 200);
		Thread.sleep(20000);
		groupMember2.setAttributes("contextATT0", 50);
	}
	
	public class GroupMemberThread implements Runnable
	{
		private final String memberName;
		private final int id;
		
		public GroupMemberThread(String name, int id)
		{
			this.memberName = name;
			this.id = id;
		}
		
		@Override
		public void run()
		{
			try
			{
				ContextSocket csocket = null;
				if(id == 1)
				{
					Map<String, Object> contextMap = new HashMap<String, Object>();
					contextMap.put("contextATT0", 50);
					
					groupMember1 = new ContextMember(memberName, contextMap);
					csocket = groupMember1.accept();
					//groupMember1.setAttributes("contextATT0", 50);
					//min = (ContextSocketInputStream) groupMember1.getInputStream();
				} else if(id == 2)
				{
					Map<String, Object> contextMap = new HashMap<String, Object>();
					contextMap.put("contextATT0", 50);
					
					groupMember2 = new ContextMember(memberName, contextMap);
					csocket = groupMember2.accept();
					
					//groupMember2.setAttributes("contextATT0", 50);
					//min = (ContextSocketInputStream) groupMember2.getInputStream();
				}
				
				System.out.println("ID "+id+"group Member working");
				byte[] userByteArray = new byte[1000];
				while(true)
				{
					//byte[] b =  min.readAny();
					int bytesRead = csocket.getInputStream().read(userByteArray);
					if(bytesRead > 0)
					{
						System.out.println("ID "+id+"Recvd "+new String(userByteArray));
					}
				}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public class GroupWriterThread implements Runnable
	{
		public final String writerName;
		public GroupWriterThread(String name)
		{
			this.writerName = name;
		}
		
		@Override
		public void run()
		{
			try
			{
				String groupName = "21 <= contextATT0 <= 147";
				
				long start = System.currentTimeMillis();
				groupWriter =  new ContextWriter(writerName, groupName);
				long end = System.currentTimeMillis();
				System.out.println("Wrtier time "+(end-start));
				
				//OutputStream gout = groupWriter.getOutputStream();
				int count = 0;
				while(true)
				{
					String hello = "hello group members of"+groupName+" count "+count++;
					groupWriter.writeAll(hello.getBytes(), 0, hello.getBytes().length);
					
					//gout.write(hello.getBytes());
					System.out.println(hello+" written");
					Thread.sleep(2000);
				}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
}