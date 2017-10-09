package hadoop.monthprotests;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * Job: List the number of protests in every country , grouped by month  (or year/month)
 * 
 * @author TChira
 * @version $Revision$
 */
public class MonthProtests {

    public static void main(String[] args){
        JobClient client=new JobClient();
        JobConf conf=new JobConf(MonthProtests.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        // specify input and output dirs
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // specify a mapper
        conf.setMapperClass(MonthProtestsMapper.class);

        // specify a reducer
        conf.setReducerClass(MonthProtestsReducer.class);
        conf.setCombinerClass(MonthProtestsReducer.class);

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
