package hadoop.influencelist;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class InfluenceReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    private Text cList = new Text();

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        String countryList = "";
        while (values.hasNext()) {
            String country = values.next().toString();
            if (!countryList.contains(country))
                countryList += country + " ";
        }

        if ((!countryList.equalsIgnoreCase("")) && (!key.toString().equalsIgnoreCase(""))) {
            cList.set(countryList);
            output.collect(key, cList);
        }

    }

}
