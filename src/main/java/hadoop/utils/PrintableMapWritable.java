package hadoop.utils;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Extended MapWritable, toString now prints every key-> value pair in the map
 * 
 * @author TChira
 * @version $Revision$
 */
public class PrintableMapWritable extends MapWritable {

    public PrintableMapWritable(){
        super();
    }
    
    @Override
    public String toString(){
        String output="";
        for(Writable key:super.keySet()){
            output+=key.toString()+" "+super.get(key).toString()+" ";
        }
        return output;
    }
}
