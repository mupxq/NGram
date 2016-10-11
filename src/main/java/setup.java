import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by mupxq on 10/10/16.
 */
public class setup extends Reducer<Text, IntWritable, DBOutputWritable, NullWritable> {
}
