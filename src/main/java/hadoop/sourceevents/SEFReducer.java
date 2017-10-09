package hadoop.sourceevents;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class SEFReducer extends MapReduceBase implements
        Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable sourceCount=new IntWritable();
    
    public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        try {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            sourceCount.set(sum);
            output.collect(key, sourceCount);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.err.println(key.toString()+"\t"+sourceCount.get());
        }
    }
}
