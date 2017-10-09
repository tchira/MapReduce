package hadoop.monthprotestsmap;

import java.io.IOException;

import hadoop.utils.PrintableMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MonthProtestsAltMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, PrintableMapWritable> {

    private Text country = new Text();
    private IntWritable one = new IntWritable(1);
  

    public void map(LongWritable key, Text value,
            OutputCollector<Text, PrintableMapWritable> output, Reporter reporter)
            throws IOException {
        try {
            PrintableMapWritable eventMap = new PrintableMapWritable();
            String line = value.toString();
            String fields[] = line.split("\\t");
            if ((Integer.parseInt(fields[27]) == 14)) // Check if event code is
                                                      // protest
            {
                String countryLoc = fields[51];
                if (!countryLoc.equalsIgnoreCase("")) {
                    String date =
                            fields[2].substring(0, 4) + "/"
                                    + fields[2].substring(4, 6); // format the date
                    country.set(countryLoc);                // country to use as primary map key
                    eventMap.put(new Text(date), one);      // date - number map to use
                                                       // as primary map value
                    System.err.println(eventMap);
                    output.collect(country, eventMap);
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}

