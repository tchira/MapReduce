package hadoop.quadcountry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * For each country, display the most frequent event class ( 4 event classes )
 * 
 * @author TChira
 * @version $Revision$
 */
public class QuadCountryDriver {

    public static void main(String[] args){
        JobClient client=new JobClient();
        JobConf conf=new JobConf(QuadCountryDriver.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        // specify input and output dirs
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        

        // specify a mapper
        conf.setMapperClass(QuadCountryMapper.class);

        // specify a reducer
        conf.setReducerClass(QuadCountryReducer.class);
     
       
        
        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
