package hadoop.sourceevents;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SEFMaxMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private Text source = new Text();

    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter) {

        try {
            String fields[] = value.toString().split(":");
            String source = fields[0];
            String eCode = fields[1];
            int count=Integer.parseInt(fields[2]);
            output.collect(new Text(source), new Text(eCode + ":" + count));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }


    }
}
