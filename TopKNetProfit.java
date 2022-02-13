// importing Libraries
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class TopKNetProfit {
    public class TopKNetProfitMapper extends
        Mapper<LongWritable, Text, Text, IntWritable>{
        
        @Override 
        public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {

            String[] tokens = value.toString().split("|");
            long sold_date = Long.parseLong(tokens[0]);
            float net_paid = Float.parseFloat(tokens[20]);
            String store = 

        }
    }

    public class TopKNetProfitReducer extends 
        Reducer<Text, IntWritable, Text, IntWritable> {
        @Override 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE; 
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get()); 
            }
            
            context.write(key, new IntWritable(maxValue));
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1); 
        }
    }
}