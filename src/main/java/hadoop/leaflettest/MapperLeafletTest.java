package hadoop.leaflettest;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MapperLeafletTest extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text leader = new Text();

    public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        String line = value.toString();
        String[] tokens = line.split("\\t");
        if (!tokens[39].contentEquals("") && !tokens[40].contentEquals("")) {
            float lat = Float.parseFloat(tokens[39]);
            float lon = Float.parseFloat(tokens[40]);
            if(lat>=-90.0 && lat<90.0 && lon>=-180.0 && lon<=180.0){
                output.collect(new Text(lat+":"+lon),one);
            }
        }
    }


}
