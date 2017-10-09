package hadoop.monthprotestsmap;

import java.io.IOException;
import java.util.Iterator;

import hadoop.utils.PrintableMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MonthProtestsReducerAlt extends MapReduceBase implements
        Reducer<Text, PrintableMapWritable, Text, PrintableMapWritable> {


    public void reduce(Text key, Iterator<PrintableMapWritable> values,
            OutputCollector<Text, PrintableMapWritable> output,
            Reporter reporter) throws IOException {

        /**
         * key : country code
         * value : Iterable collection of MapWritable<Text (date), IntWritable (Number of protests)>
         * > iterate through the keyset of every input map, if key (date) doesn't exist for current country in the output map,
         * add it with its corresponding map value ( protests). Else, increase the value in the output map by the value in the input map
         * 
         */
        PrintableMapWritable outputMap = new PrintableMapWritable();
        try {
            while (values.hasNext()) {
                MapWritable inputMap = values.next();   
                for (Writable inputKey : inputMap.keySet()) { 
                    if (!outputMap.keySet().contains(inputKey)) {
                        outputMap.put(inputKey,
                                (IntWritable) inputMap.get(inputKey)); 
                    } else {
                        int newValue =
                                ((IntWritable) outputMap.get(inputKey)).get()
                                        + ((IntWritable) inputMap.get(inputKey))
                                                .get();
                        outputMap.put(inputKey, new IntWritable(newValue));
                    }
                }
            }
            output.collect(key, outputMap);
            System.err.println(key.toString() + " " + outputMap);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
