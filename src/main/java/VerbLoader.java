
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * Created by iosifidis on 08.08.16.
 */
public class VerbLoader {
    static org.apache.log4j.Logger logger = Logger.getLogger(VerbLoader.class);

    private final HashSet<String> dictionary = new HashSet<>();

    public VerbLoader() {
        FileInputStream fstream = null;
        try {

            Path pt=new Path("verbs.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

            String strLine;
            while ((strLine = br.readLine()) != null) {
                dictionary.add(strLine);
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public HashSet<String> getDictionary() {
        return dictionary;
    }
}
