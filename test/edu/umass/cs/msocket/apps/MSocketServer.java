package edu.umass.cs.msocket.apps;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.mobility.MobilityManagerServer;

public class MSocketServer 
{
	public static void main(String[] args) throws IOException
	{
		String serverName = args[0];
		MServerSocket mserv = new MServerSocket(serverName);
		//while(true)
		{
			MSocket msocket = mserv.accept();
			OutputStream outstream = msocket.getOutputStream();
			InputStream inpstream = msocket.getInputStream();
			
			byte[] byteArray = new byte[1000];
			int i=0;
			
			while(i<10)
			{
				outstream.write( new String("hello world from server").getBytes() );
				int numRead = inpstream.read(byteArray);
				System.out.println(new String(byteArray));
				
				try
				{
					Thread.sleep(2000);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				i++;
			}
			msocket.close();
		}
		mserv.close();
		MobilityManagerServer.shutdownMobilityManager();	
	}
}