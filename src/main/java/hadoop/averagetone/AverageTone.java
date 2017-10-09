package hadoop.averagetone;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class AverageTone {
    
    public static void main(String[] args){
        
        JobClient client=new JobClient();
        JobConf conf=new JobConf(AverageTone.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        conf.setMapperClass(AverageToneMapper.class);
        conf.setReducerClass(AverageToneReducer.class);
        //conf.setCombinerClass(AverageToneReducer.class);
        
        client.setConf(conf);
        try{
            JobClient.runJob(conf);
        } catch (Exception e){
            e.printStackTrace();
        }
        
    }
}
