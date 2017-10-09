package hadoop.monthprotestsmap;

import hadoop.utils.PrintableMapWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * Job: Number of protests ( event code :14 ) in each country, grouped by month
 * Alternate implementation using a PrintableMapWritable ( extended MapWritable with toString() )
 * 
 * @author TChira
 * @version $Revision$
 */
public class MonthProtestsAlt {

    public static void main(String[] args){
        JobClient client=new JobClient();
        JobConf conf=new JobConf(MonthProtestsAlt.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(PrintableMapWritable.class);
        // specify input and output dirs
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        

        // specify a mapper
        conf.setMapperClass(MonthProtestsAltMapper.class);

        // specify a reducer
        conf.setReducerClass(MonthProtestsReducerAlt.class);
       conf.setCombinerClass(MonthProtestsReducerAlt.class);
       
        
        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
