package edu.umass.cs.gnsserver.activecode.prototype.channels;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Message;
import org.json.JSONException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class ActiveNamedPipe implements Channel {
	
	private InputStream reader;
	private OutputStream writer;
	
	byte[] readerLengthBuffer = new byte[Integer.BYTES];
	byte[] writerLengthBuffer = new byte[Integer.BYTES];
	

	public ActiveNamedPipe(String ifile, String ofile){
		Thread t = new Thread(new Runnable() {
	         public void run()
	         {
	        	try {
					reader = new FileInputStream(new File(ifile));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
	         }
		});
		t.start();
		
		try {			
			writer = new FileOutputStream(new File(ofile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public synchronized void sendMessage(Message msg) throws IOException {
		byte[] buf = msg.toBytes();
		int length = buf.length;
		try {
			// write the length of byte array first then send the content
			
			writer.write(ByteBuffer.allocate(Integer.BYTES+length).putInt(length).put(buf).array());
			writer.flush();
		} catch (IOException e) {			
			//e.printStackTrace();
		}
	}

	@Override
	public Message receiveMessage() throws IOException {
		Message am = null;
		int len = -1;
		if(reader != null)
			len = reader.read(readerLengthBuffer, 0, readerLengthBuffer.length);
		if(len>0){
			int length = ByteBuffer.wrap(readerLengthBuffer).getInt();
			byte[] buffer = new byte[length];
			reader.read(buffer, 0, length);			
			try {
				am = new ActiveMessage(buffer);
			} catch (JSONException e) {
				//e.printStackTrace();
			}
		}
		
		return am;
	}
	
	@Override
	public void close() {
		try{
			if(reader != null)
				reader.close();
			if(writer != null)
				writer.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
