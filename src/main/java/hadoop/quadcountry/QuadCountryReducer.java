package hadoop.quadcountry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class QuadCountryReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> value,
            OutputCollector<Text, Text> output, Reporter reporter) {
        Map<String, Integer> codeMap = new HashMap<String, Integer>();
        int maxCount = 0;
        String maxCode = null;

        while (value.hasNext()) {
            String code = value.next().toString();
            if (codeMap.containsKey(code)) {
                codeMap.put(code, codeMap.get(code) + 1);
            } else {
                codeMap.put(code, 1);
            }

        }
        for (String ckey : codeMap.keySet()) {
            if (codeMap.get(ckey) > maxCount) {
                maxCount = codeMap.get(ckey);
                maxCode = ckey;
            }
        }
        try {
            output.collect(key, new Text(maxCode));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
