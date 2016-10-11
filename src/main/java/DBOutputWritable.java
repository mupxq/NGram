import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by mupxq on 10/10/16.
 */
public class DBOutputWritable  implements DBWritable{

    private String starting_phrase;
    private String follow_word;
    private int count;

    @Override
    public void write(PreparedStatement arg0) throws SQLException {

        arg0.setString(1, starting_phrase);
        arg0.setString(2, follow_word);
        arg0.setInt(3, count);
    }

    @Override
    public void readFields(ResultSet arg0) throws SQLException {
        this.starting_phrase = arg0.getString(1);
        this.follow_word = arg0.getString(2);
        this.count = arg0.getInt(3);
    }

    public static class Reduce extends setup {
        int n;
        @Override
        public void setup(Context context){
        Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);

        }

        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, List<String >> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text val: values){
                String curValue = val.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());
                if (tm.containsKey(count)){
                    tm.get(count).add(word);
                }
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();


        }
    }
}
