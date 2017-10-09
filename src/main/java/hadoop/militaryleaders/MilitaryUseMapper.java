package hadoop.militaryleaders;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MilitaryUseMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text leader = new Text();

    public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        String line = value.toString();
        String[] tokens = line.split("\\t");
        if (tokens[26].contentEquals("190")&&tokens[12].contentEquals("GOV")) {
            leader.set(tokens[6]);
            output.collect(leader, one);
        }
    }
}
