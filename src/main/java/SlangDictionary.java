
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by iosifidis on 07.08.16.
 */
public class SlangDictionary {
    private final HashMap<String, String> dictionary = new HashMap<>();
    public SlangDictionary() {
        FileInputStream fstream = null;
        try {

            Path pt=new Path("slang_dictionary.txt");
            FileSystem fs = FileSystem.get(new Configuration());

            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

            String strLine;
            while ((strLine = br.readLine()) != null) {
                dictionary.put(strLine.split("-")[0], strLine.split("-")[1].replaceAll("  ",""));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, String> getDictionary() {
        return dictionary;
    }
}
