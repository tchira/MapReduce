package hadoop.gratio;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class GRatioMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private Text country1 = new Text();
    private Text country2 = new Text();
    private Text gScore = new Text();

    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        String fields[] = line.split("\\t");

        String actor1Country = fields[7];
        String actor2Country = fields[17];
        String goldstein = fields[30];
        if (!actor1Country.contentEquals("") && !actor2Country.contentEquals("")) {
            if (actor1Country.contentEquals(actor2Country)) {
                country1.set(actor1Country);
                gScore.set("i " + goldstein);
                output.collect(country1, gScore);
                System.err.println(country1.toString()+" "+gScore.toString());
            } else {
                country1.set(actor1Country);
                country2.set(actor2Country);
                gScore.set("e " + goldstein);
                output.collect(country1, gScore);
                output.collect(country2, gScore);
                System.err.println(country1.toString()+" "+gScore.toString());
                System.err.println(country2.toString()+" "+gScore.toString());
            }
        }

    }


}