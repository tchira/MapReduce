package hadoop.sourceevents;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class SEFMaxReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, IntWritable> {

    private IntWritable maxCount = new IntWritable();

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        try {

            int max = -1;
            String maxEvent = "";
            
            while (values.hasNext()) {
                
                String fields[] = values.next().toString().split(":");
                System.err.print(key + " " + fields[0]+" "+fields[1]);
                String eCode = fields[0];
                int sourceCount = Integer.parseInt(fields[1]);
                if (sourceCount > max) {
                    max = sourceCount;
                    maxEvent = eCode;
                }
            }
            maxCount.set(max);
            output.collect(new Text(key+", event code:"+maxEvent), maxCount);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.print(key + " " + maxCount.get());
        }
    }
}
