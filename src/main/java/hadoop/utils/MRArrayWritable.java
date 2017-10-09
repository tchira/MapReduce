package hadoop.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;


public class MRArrayWritable extends ArrayWritable {

    public MRArrayWritable() {
        super(Text.class);
    }

    public MRArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }

}