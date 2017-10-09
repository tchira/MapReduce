package hadoop.sourceevents;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SEFMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {

    private Text source = new Text();
    private IntWritable sCount = new IntWritable();

    public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter) {
        try {
            String line = value.toString();
            String[] tokens = line.split("\\t");
            String nSource = tokens[57];
            int endIndex;
            if (nSource.contains("https")) {
                endIndex = nSource.indexOf("/", 9);
                if (endIndex < 0)
                    source.set(nSource.substring(8, nSource.length()));
                else
                    source.set(nSource.substring(8, endIndex));
            } else if (nSource.contains("http")) {
                endIndex = nSource.indexOf("/", 8);
                if (endIndex < 0)
                    source.set(nSource.substring(7, nSource.length()));
                else
                    source.set(nSource.substring(7, endIndex));
            } else {
                source.set(tokens[57]);
            }
            if (!source.toString().contentEquals("")) {
                source.set(source.toString() +":"+tokens[26]);
                sCount.set(Integer.parseInt(tokens[32]));
                output.collect(source, sCount);
            }
        } catch (Exception e) {

            System.err.println(e.getMessage() + " " + value.toString());
            e.printStackTrace();
        }
    }
}
