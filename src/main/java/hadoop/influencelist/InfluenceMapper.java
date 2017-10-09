/**
 * GDELT : 38 : Actor 1 country, 45 : Actor 2 country
 * Country 1 has an action upon country 2
 */
package hadoop.influencelist;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class InfluenceMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private Text country = new Text();
    private Text influenced = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output,
            Reporter reporter) throws IOException {
            
        String line=value.toString();
        String tokens[]=line.split("\\t");
        country.set(tokens[37]);
        influenced.set(tokens[44]);
        output.collect(country, influenced);
        
    }

}
