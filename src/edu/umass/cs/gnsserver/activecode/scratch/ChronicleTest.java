package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;

public class ChronicleTest {
	
	public static void main(String[] args) {
		
		int size = Integer.parseInt(args[0]);
		
		String dir = "/tmp/test";
		ChronicleQueue queue = ChronicleQueueBuilder.single(dir).build();
		
		byte[] b = new byte[size];
		new Random().nextBytes(b);
		final String msg = new String(b);//"Hello world!";
		
		try{
		    // Obtain an ExcerptAppender
		    ExcerptAppender appender = queue.acquireAppender();
		    ExcerptTailer tailer = queue.createTailer();
		    
		    // write - {msg: TestMessage}
		    
		    int n = 1000000;
		    long t = System.currentTimeMillis();
		    for(int i=0; i<n; i++){
		    	appender.writeDocument(w -> w.write(() -> "msg").text(msg));		    
		    	
			    tailer.readDocument(w -> w.read(()->"msg").text());
		    }
		    
		    long elapsed = System.currentTimeMillis()-t;
			
			System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		    
			/*
			Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
			unsafeConstructor.setAccessible(true);
			Unsafe unsafe = unsafeConstructor.newInstance();
			
			byte[] bytes = msg.getBytes(StandardCharsets.ISO_8859_1);
		    t = System.currentTimeMillis();
		    for(int i=0; i<10; i++){
		    	unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, 0, bytes.length);
		    }
		    elapsed = System.currentTimeMillis()-t;
			
			System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
			*/
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			queue.close();
			
			for(File file: new File(dir).listFiles()) 
			    if (!file.isDirectory()) 
			        file.delete();
			
		}
		
	}
}
