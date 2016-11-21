package edu.umass.cs.wiki;

import org.json.JSONArray;
import org.json.JSONException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes {@link WikiConstants} to a JSON file
 * @author Karthik
 */
public class WikiConstantsGenerator {

    /**
     * If the filename has to be changed, it has to be also updated in the script
     * that reads the generated file to modify wiki pages
     */
    public static final String WIKI_CONSTANTS_FILE_PATH = "scripts"+System.getProperty("file.separator")+"wiki"+System.getProperty("file.separator")+"wiki_constants.json";

    /**
     * The core method to generate wiki constants JSON file
     */
    private static void generateWikiConstants() {

        /* Create directory tree if it doesn't exist*/
        File file = new File(WIKI_CONSTANTS_FILE_PATH);
        file.getParentFile().mkdirs();

        FileWriter fileWriter = null;
        JSONArray jsonData = null;
        try {
            jsonData = new JSONArray(WikiConstants.values());
            fileWriter = new FileWriter(WIKI_CONSTANTS_FILE_PATH);
            fileWriter.write(jsonData.toString(2));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            try {
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * The main method to invoke wiki constants generator
     * @param args
     */
    public static void main(String[] args) {
        generateWikiConstants();
    }
}
