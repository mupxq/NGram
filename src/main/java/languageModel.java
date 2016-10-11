/**
 * Created by mupxq on 10/8/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class languageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        int threadshold;
        @Override
        public void setup(Context context){
        Configuration conf = context.getConfiguration();
            threadshold = conf.getInt("threadshold", 20);


        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if ((value == null) || (value.toString().trim().length() == 0)){
                return;
            }
            String line = value.toString().trim();

            String[] wordsPlusCount = line.split("\t");
            if (wordsPlusCount.length < 2){
                return;
            }

            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.valueOf(wordsPlusCount[1]);

            if (count < threadshold){
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length-1; i++){
                sb.append(words[i]).append(" ");
            }
            String outputkey = sb.toString().trim();
            String outputValue = words[words.length-1];

            if (!((outputkey == null) || (outputkey.length() < 1))){
                context.write(new Text(outputkey), new Text(outputValue + "=" + count));
            }

        }
    }


    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, IntWritable> {


        public void reduce(Text key,Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {


        }
    }
}
