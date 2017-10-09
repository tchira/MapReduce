/**
 * Task: For every news article (source) find the average tone (34) (57)
 * if tone > 0, output positive, else output negative
 */

package hadoop.averagetone;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Parse input line, chop the source link (field 57) to get just the main site
 * Output collects site and the article tone ( float : field 34 )
 * 
 * @author TChira
 * @version $Revision$
 */
public class AverageToneMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, FloatWritable> {

    private FloatWritable tone = new FloatWritable();
    private Text source = new Text();

    public void map(LongWritable key, Text value,
            OutputCollector<Text, FloatWritable> output, Reporter reporter)
            throws IOException {

        try {
            String line = value.toString();
            String[] tokens = line.split("\\t");
            String nSource = tokens[57];
            int endIndex;
            if (nSource.contains("http")) {
                endIndex = nSource.indexOf("/", 8);
                if (endIndex < 0)
                    source.set(nSource.substring(7, nSource.length()));
                else
                    source.set(nSource.substring(7, endIndex));
            } else if (nSource.contains("https")) {
                endIndex = nSource.indexOf("/", 9);
                if (endIndex < 0)
                    source.set(nSource.substring(8, nSource.length()));
                else
                    source.set(nSource.substring(8, endIndex));
            } else {
                source.set(tokens[57]);
            }

            tone.set(Float.parseFloat(tokens[34]));
            output.collect(source, tone);
        } catch (Exception e) {

            System.err.println(e.getMessage() + " " + value.toString());
            e.printStackTrace();
        }
    }


}
