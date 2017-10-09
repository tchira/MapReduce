//Lists all the government officials ( actor type GOV ) that have used military power ( event code =190)
package hadoop.militaryleaders;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * Task: count how many events happened in each country
 * 
 * @author TChira
 * @version $Revision$
 */
public class MilitaryUseCount {
    
    public static void main(String[] args){
        JobClient client=new JobClient();
        JobConf conf=new JobConf(MilitaryUseCount.class);
     // specify output types
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // specify input and output dirs
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // specify a mapper
        conf.setMapperClass(MilitaryUseMapper.class);

        // specify a reducer
        conf.setReducerClass(MilitaryUseReducer.class);
        conf.setCombinerClass(MilitaryUseReducer.class);

        client.setConf(conf);
        try {
          JobClient.runJob(conf);
        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
