package edu.umass.cs.gnsserver.activecode.prototype.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import static java.lang.Math.toIntExact;

/**
 * This is a simple implementation of a circular buffer with {@code RandomAccessFile}.
 * <p> The writer writes the data into the shared memory from the current position of the
 * writer, if the end of the file is about to be reached, set a negative integer and
 * write from the beginning of the file. To write from the beginning, the writer needs
 * to make sure the reader has already read the data. 
 * 
 * <p> The reader reads the data from the shared memory from the current position of the
 * reader, of it reads a negative value of the length of the target array, it knows 
 * 
 * @author gaozy
 *
 */
public class CircularBufferedRandomAccessFile {
	
	private boolean enableDebug = true;
	
	/**
	 * Only write operation will increase the value of this variable.
	 */
	private AtomicLong writerPosition = new AtomicLong();
	/**
	 * Only read operation will increase the value of this variable.
	 */
	private AtomicLong readerPosition = new AtomicLong();
	/**
	 * The size of the file
	 */
	private final long size;
	
	/**
	 * Underlying file
	 */
	private RandomAccessFile mem;
	private RandomAccessFile header;
	private String file;
	
	// default size 1MB
	private final static long MAX_SIZE = 1000000;
	
	private final byte[] zeros;
	
	private final String id;
	
	/**
	 * The first 32 byte is used for writing header of the channel
	 */
	private final static long HEADER_LENGTH = Long.BYTES;
	
	/**
	 * @param file
	 * @param size
	 * @param enableDebug
   * @param id
	 */
	public CircularBufferedRandomAccessFile(String file, long size, boolean enableDebug, String id){
		this.file = file;
		this.size = size;
		this.enableDebug = enableDebug;
		this.id = id;
		
		zeros = new byte[toIntExact(size)];
		Arrays.fill( zeros, (byte) 0 );
		
		try {
			File f = new File(file);
			mem = new RandomAccessFile(f, "rw");
			mem.setLength(size);
			mem.seek(HEADER_LENGTH);
			
			header = new RandomAccessFile(f, "rw");
			
		} catch ( IOException e) {
			e.printStackTrace();
		}
		
		writerPosition.set(HEADER_LENGTH);
		readerPosition.set(HEADER_LENGTH);
		
	}
	
	/**
	 * @param file
	 */
	public CircularBufferedRandomAccessFile(String file){
		this(file, MAX_SIZE, false, "");	
	}
	
	
	/**
	 * @param file
	 * @param enableDebug
   * @param id
	 */
	public CircularBufferedRandomAccessFile(String file, boolean enableDebug, String id){
		this(file, MAX_SIZE, enableDebug, id);
	}
	
	/**
	 * Delete the file if it already exists
	 * @param file
	 */
	private static void cleanup(String file){
		if((new File(file)).exists()){
			(new File(file)).delete();
		}		
	}
	
	/**
	 * @param data data to write
	 * @throws IOException
	 */
	public synchronized void write(byte[] data) throws IOException {
		/**
		 * The total length of bytes to be written, there should a
		 * integer byte left for the writer to inform reader that
		 * the end of the file has been reached.
		 */
		int length = data.length + Integer.BYTES + Integer.BYTES;		
		
		waitUntilWritable(length);
		
		/**
		 * If the written length of data is longer than the 
		 * file length from the current position, set a length
		 * to negative then write from the beginning of the file.
		 */
		if(length + writerPosition.get() > size ){
			byte[] buf = ByteBuffer.allocate(Integer.BYTES).putInt(-1).array();
			mem.write(buf, 0, buf.length);
			
			//System.out.println(length+"=2*"+Integer.BYTES+"+"+data.length+",writer position:"+writerPosition.get());
			
			writerPosition.set(HEADER_LENGTH);
			mem.seek(writerPosition.get());
			
		}
		writeBytes(data);
		
	}
	
	
	private void waitUntilWritable(int length){
		/**
		 * If the writer position will exceed the reader position, wait until the reader catches up
		 */
		
		while( ((writerPosition.get() >= readerPosition.get())&&(writerPosition.get()+length>size)&&(HEADER_LENGTH+length-Integer.BYTES >= readerPosition.get()))
				|| ((writerPosition.get() < readerPosition.get()) && (writerPosition.get()+length - Integer.BYTES >= readerPosition.get()))){
			//System.out.println("writer:"+writerPosition.get()+"\n reader:"+readerPosition.get());
			readerPosition.set(ByteBuffer.wrap(getHeader()).getLong());
		}
	}
	
	private void writeBytes(byte[] data) throws IOException{
		if(enableDebug)
			System.out.println(id+" write:"+data.length+"+"+Integer.BYTES+" at "+writerPosition.get());
		byte[] buffer = ByteBuffer.allocate(Integer.BYTES).putInt(data.length).array();
		mem.write(buffer, 0, Integer.BYTES);
		mem.write(data, 0, data.length);
		writerPosition.set(writerPosition.get()+Integer.BYTES+data.length);
	}
	
	/**
	 * @return bytes read
	 * @throws IOException
	 */
	public byte[] read() throws IOException {
		byte[] buffer = new byte[Integer.BYTES];
		mem.read(buffer);
		int length = ByteBuffer.wrap(buffer).getInt();
		if(enableDebug)
			System.out.println(id+" read: "+length+" bytes");
		
		if(length < 0){
			
			reset(readerPosition.get(), Integer.BYTES);
			
			byte[] bbuf = new byte[Integer.BYTES];
			mem.seek(HEADER_LENGTH);
			mem.read(bbuf);
			length = ByteBuffer.wrap(bbuf).getInt();
			if (length == 0){
				mem.seek(HEADER_LENGTH);
				readerPosition.set(HEADER_LENGTH);
				return null;
			}
			if(enableDebug)
				System.out.println(id+" read new length:"+length+" bytes");
			byte[] data = new byte[length];
			mem.read(data);
			
			reset(HEADER_LENGTH, Integer.BYTES+length);
			
			readerPosition.set(HEADER_LENGTH+Integer.BYTES+length);
			resetHeader(readerPosition.get());
			
			return data;
		}else if(length == 0){
			mem.seek(readerPosition.get());
			return null;
		}else{
			
			byte[] data = new byte[length];
			mem.read(data);
			reset(readerPosition.get(), Integer.BYTES+length);
			
			readerPosition.set(readerPosition.get()+Integer.BYTES+length);
			resetHeader(readerPosition.get());
			
			return data;
		}
	}
	
	/**
	 * The reason we do not use this reset method is because the read method is
	 * relatively slower than write method, and write can always catch up read.
	 * But if it is not true, the bug will happen immediately
	 * @param pos
	 * @param length
	 * @throws IOException
	 */
	private synchronized void reset(long pos, int length) throws IOException{
		if(enableDebug)
			System.out.println(id+" reset:"+pos+" "+length);
		mem.seek(pos);
		mem.write(zeros, 0, length);
	}
	
	private synchronized void resetHeader(long readerPos) throws IOException{
		byte[] hbuf = ByteBuffer.allocate(Long.BYTES).putLong(readerPos).array();
		header.write(hbuf);
		header.seek(0);
	}
	
	private byte[] getHeader(){
		byte[] buf = new byte[Long.BYTES];
		try {
			header.read(buf);
			header.seek(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return buf;
	}
	
	/**
	 * Shut things down.
	 */
	public void shutdown() {
		try {
			mem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		cleanup(file);
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException{
		/*
		byte[] buf1 = "hello world!4".getBytes();
		ByteBuffer bbuf = ByteBuffer.allocate(Integer.BYTES+13);
		bbuf.putInt(13).put(buf1).flip();
		int l = bbuf.getInt();
		System.out.println(l);
		
		// test for the bug
		ByteBuffer bbuf1 = ByteBuffer.allocate(1+Integer.BYTES);
		bbuf1.put("0".getBytes());
		bbuf1.putInt(13);
		bbuf1.flip();
		int l = bbuf1.getInt();
		System.out.println(l);
		*/
		
		String cfile = "/tmp/test";
		
		CircularBufferedRandomAccessFile client = new CircularBufferedRandomAccessFile(cfile);
		
		CircularBufferedRandomAccessFile server = new CircularBufferedRandomAccessFile(cfile);
		
		int length = 32;
		byte[] buf = new byte[length];
		new Random().nextBytes(buf);
		
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		for(int i=0; i<n; i++){
			
			client.write(buf);
			//byte[] buf2 = 
			server.read();
			//System.out.println(new String(buf2));
		}
		long eclapsed = System.currentTimeMillis() -t;
		System.out.println("It takes "+eclapsed+"ms.");
		client.shutdown();
		server.shutdown();
	}
}
