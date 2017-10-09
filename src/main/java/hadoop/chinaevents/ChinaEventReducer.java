package hadoop.chinaevents;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/**
 * Input : Pairs of <"Year/Week in Year",1>
 * Reducer/Combiner adds all the map values to output total number of events for each Week
 * 
 * @author TChira
 * @version $Revision$
 */
public class ChinaEventReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {

    int numberOfEvents = 0;
    while (values.hasNext()) {
      IntWritable value = (IntWritable) values.next();
      numberOfEvents += value.get(); // process value
    }

    output.collect(key, new IntWritable(numberOfEvents));
  }
}