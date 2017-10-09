package hadoop.sourceevents;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


/**
 * For each news source, find the most focused event type, by checking the total numSources for each event code
 * Uses 2 linked jobs: 
 * 1. parse the CSV file, outputs pairs of <"Country:event code", numSources>
 * 2. gets the input from 1, compute the maximum numSources for each country
 *    output: <"country, event code: <ecode>", <number of sources> > pairs
 * 
 * @author TChira
 * @version $Revision$
 */
public class SourceEventFocus {

    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(SourceEventFocus.class);
        conf.set("mapred.textoutputformat.separator", ":");
       conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        // specify input and output dirs
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path("tchira/temp"));

        // specify a mapper
        conf.setMapperClass(SEFMapper.class);

        // specify a reducer
        conf.setReducerClass(SEFReducer.class);
        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
       
        JobConf conf2 = new JobConf(SourceEventFocus.class);
        conf2.setMapOutputKeyClass(Text.class);
        conf2.setMapOutputValueClass(Text.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(IntWritable.class);
      
        // specify input and output dirs
        FileInputFormat.addInputPath(conf2, new Path("tchira/temp"));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

     
        conf2.setMapperClass(SEFMaxMapper.class);
        conf2.setReducerClass(SEFMaxReducer.class);
        client.setConf(conf2);
        try {
            JobClient.runJob(conf2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
