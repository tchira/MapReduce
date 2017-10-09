package hadoop.averagetone;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.Iterator;

/**
 * Get source site as key and the article tones (floats) as values
 * Collect <source, positive / negative> pairs, positive if toneaverage>0, else negative
 * tone average= sum of all article tones for that source / number of articles

 * @author TChira
 * @version $Revision$
 */
public class AverageToneReducer extends MapReduceBase implements
        Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterator<FloatWritable> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        float toneSum = 0;
        int toneCount = 0;
        Text result = new Text();
        while (values.hasNext()) {
            FloatWritable value = (FloatWritable) values.next();
            toneCount++;
            toneSum += value.get();
        }

        float average = toneSum / toneCount;
        if (average > 0) {
            result.set("positive");
        } else {
            result.set("negative");
        }
        output.collect(key, result);
    }

}
