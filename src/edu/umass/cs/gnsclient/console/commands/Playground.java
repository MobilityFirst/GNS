package edu.umass.cs.gnsclient.console.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by tramteja on 4/17/17.
 */
public class Playground {
    public static void main(String args[]){
        String str = "Location \"Welcome  to india   \" Bangalore " +
                "Channai \"IT city\"  Mysore";

        List<String> list = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(str);
        while (m.find())
            list.add(m.group(1).replace("\"", "").trim()); // Add .replace("\"", "") to remove surrounding quotes.
        System.out.println(list);
        System.out.println(list.size());

    }
}
