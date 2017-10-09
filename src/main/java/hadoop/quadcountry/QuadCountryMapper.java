package hadoop.quadcountry;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class QuadCountryMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key,Text value,OutputCollector<Text,Text> output,Reporter reporter){
            try {
                String fields[]=value.toString().split("\\t");
                output.collect(new Text(fields[51]), new Text(fields[29]));
            } catch (IOException e) {
                
                e.printStackTrace();
            }
        }
}
