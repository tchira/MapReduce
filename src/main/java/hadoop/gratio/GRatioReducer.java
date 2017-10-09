package hadoop.gratio;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class GRatioReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        try {
            float internalSum = 0;
            int internalCount = 0;
            float externalSum = 0;
            int externalCount = 0;

            while (values.hasNext()) {
                String[] fields = values.next().toString().split(" ");
                float goldstein = Float.parseFloat(fields[1]);
                if (fields[0].contentEquals("e")) {
                    externalSum += goldstein;
                    externalCount++;
                } else {
                    internalSum += goldstein;
                    internalCount++;
                }

            }
            float intAverage = 0.0f;
            float extAverage = 0.0f;
            if (internalCount != 0)
                intAverage = internalSum / internalCount;

            if (externalCount != 0)
                extAverage = externalSum / externalCount;

            output.collect(key, new Text(intAverage + " " + extAverage));


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
