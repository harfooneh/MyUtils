package datamining.apache.mahout;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by a.zarifi on 7/22/14.
 */
public class readMahoutSequenceFile {

    public readMahoutSequenceFile() {
        Configuration conf = new Configuration();
        FileSystem fs;
        SequenceFile.Reader read;
        try {
            fs = FileSystem.get(conf);
            read = new SequenceFile.Reader(fs, new Path("/Sparsedir/dictionary.file-0"), conf);
            IntWritable dicKey = new IntWritable();
            Text text = new Text();
            HashMap dictionaryMap = new HashMap();
            try {
                while (read.next(text, dicKey)) {
                    dictionaryMap.put(Integer.parseInt(dicKey.toString()), text.toString());
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            read.close();

            read = new SequenceFile.Reader(fs, new Path("/Sparsedir/tf-vectors/part-r-00000"), conf);
            Text key = new Text();
            VectorWritable value = new VectorWritable();
            SequentialAccessSparseVector vect;
            while (read.next(key, value)) {
                NamedVector namedVector = (NamedVector) value.get();
                vect = (SequentialAccessSparseVector) namedVector.getDelegate();
                for (Vector.Element e : vect) {
                    System.out.println("Token: " + dictionaryMap.get(e.index()) + ", TF-IDF weight: " + e.get());
                }
            }
            read.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
