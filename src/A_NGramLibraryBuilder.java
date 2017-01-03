/**
 * Created by zackpeng on 11/11/16.
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
Eg. noGram == 2;
Mapper:
Input:
text:   I love New York. Really love it.

Output:
Key             Value
i love          1
love new        1
new york        1
york really     1
really love     1
love it         1


Reducer
Output:
i love          1 + 1 + 1 + ... =its sum

Same as Word Count.
*/

public class A_NGramLibraryBuilder {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //Number of Gram
        int noGram;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram", 5);      //?
        }

        //map method
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            line = line.trim().toLowerCase();
            line = line.replaceAll("[^a-z]+", " ");
            String words[] = line.split("\\s+"); //split by ' ', '\t', '\n', etc.

            if(words.length < 2) {
                return;
            }

            StringBuilder sb;
            for (int i = 0; i < words.length - 1; i++) {
                sb = new StringBuilder();
                for (int j = 0;  i + j < words.length && j < noGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //reduce method
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}