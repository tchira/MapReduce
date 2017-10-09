/** 
 * Task : for each country (country1), get the list of countries (country 2) influenced by it
 */
package hadoop.influencelist;

import hadoop.utils.MRArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class InfluenceCount {

    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(InfluenceCount.class);
        
        //        conf.set(name, value);
        
  
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MRArrayWritable.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        // specify input and output dirs
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // specify a mapper
        conf.setMapperClass(InfluenceMapper.class);

        // specify a reducer
        conf.setReducerClass(InfluenceReducer.class);
        //conf.setCombinerClass(InfluenceReducer.class);

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
