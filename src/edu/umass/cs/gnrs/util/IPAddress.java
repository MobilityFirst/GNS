package edu.umass.cs.gnrs.util;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/1/13
 * Time: 11:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class IPAddress {

    public static  void  main(String[] args) {

        String filename = args[0];
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            while(true) {
                String ipAddressString = br.readLine().trim();
                if (ipAddressString == null)
                    break;

                InetAddress ipAddress = null;
                try {
                    ipAddress = InetAddress.getByName(ipAddressString);
                    System.out.println(ipAddress.getHostAddress());
                } catch (UnknownHostException e) {
                    System.err.println("Problem parsing IP address for NS " + ipAddressString + " :" + e);
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}

