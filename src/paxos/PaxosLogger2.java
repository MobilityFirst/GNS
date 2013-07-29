package paxos;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartNameServer;
import edu.umass.cs.gnrs.packet.paxospacket.PaxosPacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/24/13
 * Time: 8:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class PaxosLogger2 extends Thread{
    /**
     * lock of the message queue
     */
    private static final Object msgQueueLock = new ReentrantLock();

    /**
     *
     */
    private static ArrayList<JSONObject> msgs = new ArrayList<JSONObject>();

    public static String logFolder = null;

    static String logFileName = null;

    static FileWriter fileWriter;

    public static void initLogger() {

        if (logFolder == null) {
            System.out.println("Specify paxos log folder. Can't be null ");
            System.exit(2);
            return;
        }

        File f = new File(logFolder);
        if (!f.exists()) {
            f.mkdirs();
        }

        logFileName = logFolder + "/paxoslog";
        if (StartNameServer.debugMode) GNS.getLogger().fine(" Logger Initialized.");
        try {
            fileWriter = new FileWriter(logFileName);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (StartNameServer.debugMode) GNS.getLogger().fine(" File Writer created.");
        LoggingThread thread = new LoggingThread();
        thread.start();
        if (StartNameServer.debugMode) GNS.getLogger().fine(" Thread started.");
    }

    /**
     * Add a msg to logging queue
     * @param jsonObject
     */
    public static void logMessage(JSONObject jsonObject) {
        synchronized (msgQueueLock) {
            msgs.add(jsonObject);
        }
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" Added msg to queue: " + jsonObject);
    }

    /**
     * log the current msgs in queue
     */
    static void doLogging() {
        while(true) {




            ArrayList<JSONObject> msgsLogged = null;

            synchronized (msgQueueLock) {
                if (msgs.size() > 0){
                    msgsLogged = msgs;
                    msgs = new ArrayList<JSONObject>();
                }

            }

            if (msgsLogged == null) {

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;

            }
    //        if (StartNameServer.debugMode) GNS.getLogger().fine(" Logging messages: " + msgsLogged.size());
            // log the msgs

//            StringBuilder sb = new StringBuilder();
            char[] buf = new char[1000];
            int index = 0;
            for (JSONObject jsonObject: msgsLogged) {

//                StringBuilder sb = new StringBuilder();
//                for (Iterator i = jsonObject.keys(); i.hasNext();) {
//                    try {
//                        sb.append(jsonObject.get((String) i.next())+ " ");
//                    } catch (JSONException e) {
//                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                    }
//
//                }
//                sb.append("\n");

                try {
                    String s = jsonObject.toString();
                    for (int i = 0; i < 20; i++) {
                        buf[index] = s.charAt(i);
                        index++;
                    }
                    buf[index]  = "\n".charAt(0);
                    index++;

                    if (index > 900) {
                        buf[index]  = "\n".charAt(0);
                        index++;
                        fileWriter.write(buf, 0, index);
                        index = 0;
                    }
//                    sb.append(jsonObject.toString().substring(0,20) + "\n");
//                    if (sb.length() > 900) {
//                        fileWriter.write(sb.toString());
//                        sb = new StringBuilder();
//                    }
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }


            }

            try {
                if (index > 0) {
                    buf[index]  = "\n".charAt(0);
                    index++;
                    fileWriter.write(buf, 0, index);
                }
                fileWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            // process each msg

            for (JSONObject jsonObject : msgsLogged) {
                try {
                    PaxosManager.executorService.submit(new HandlePaxosMessageTask(jsonObject, jsonObject.getInt(PaxosPacketType.ptype)));
                } catch (JSONException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

}


class LoggingThread extends Thread{
    @Override
    public void run(){
        PaxosLogger2.doLogging();
    }
}