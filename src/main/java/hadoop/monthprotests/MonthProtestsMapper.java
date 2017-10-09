package hadoop.monthprotests;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MonthProtestsMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {

    private Text countryMonth = new Text();
    private IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output,
            Reporter reporter) throws IOException {
        String line = value.toString();
        String fields[] = line.split("\\t");
        if ((Integer.parseInt(fields[27]) == 14)) // Check if event code is
                                                  // protest
        {
            String countryLoc = fields[51];
            if (!countryLoc.equalsIgnoreCase("")) {
                String date =
                        fields[2].substring(0, 4) + "/"
                                + fields[2].substring(4, 6); //format the date
                countryMonth.set(countryLoc + " " + date);
                output.collect(countryMonth, one);
            }
        }
    }


}
