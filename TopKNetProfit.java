
// importing Libraries
import java.io.IOException;
// import java.util.Map;
// import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKNetProfit {
    public static class TopKNetProfitMapper extends
            Mapper<LongWritable, Text, Text, FloatWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            // we will use the value passed in start date and end date at runtime
            long start_date = Long.parseLong(conf.get("start_date"));
            long end_date = Long.parseLong(conf.get("end_date"));

            String[] tokens = value.toString().split("|");
            long sold_date = Long.parseLong(tokens[0]);
            String store = tokens[7];
            float net_paid = Float.parseFloat(tokens[20]);

            if (sold_date > start_date && sold_date < end_date) {
                context.write(new Text(store), new FloatWritable(net_paid));
            }

        }
    }

    public static class TopKNetProfitReducer extends
            Reducer<Text, FloatWritable, Text, FloatWritable> {
            
            private FloatWritable result = new FloatWritable();

        // private TreeMap<Float, String> treemap;

        // public void setup(Context context) throws IOException,
        // InterruptedException {
        // treemap = new TreeMap<Float, String>();
        // }

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            // Configuration conf = context.getConfiguration();
            // we will use the value passed in start date and end date at runtime
            // long k = Long.parseLong(conf.get("K"));

            // String store = key.toString();
            float storeNetProfitSum = 0;
            for (FloatWritable value : values) {
                storeNetProfitSum = storeNetProfitSum + value.get();
            }
            result.set(storeNetProfitSum);
            context.write(key, result);

            // treemap.put(storeNetProfitSum, store);
            // if (treemap.size() > k) {
            // treemap.remove(treemap.firstKey());
            // }
        }

        // public void cleanup(Context context) throws IOException,
        // InterruptedException {

        // for (Map.Entry<Float, String> entry : treemap.entrySet()) {
        // float netProfit = entry.getKey();
        // long store = Long.parseLong(entry.getValue());
        // context.write(new LongWritable(store), new FloatWritable(netProfit));
        // }
        // }

    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 5) {
            System.err.println(
                    "Usage: Top Net Profit <K> <start_date> <end_date> <input_file> <output_path>");
            System.exit(-1);
        }

        String k = args[0];
        String start_date = args[1];
        String end_date = args[2];
        String source = args[3];
        String dest = args[4];

        Configuration conf = new Configuration();
        conf.set("K", k);
        conf.set("start_date", start_date);
        conf.set("end_date", end_date);

        Job job = Job.getInstance(conf, "TopK");
        job.setJarByClass(TopKNetProfit.class);
        job.setMapperClass(TopKNetProfitMapper.class);
        job.setReducerClass(TopKNetProfitReducer.class);

        // job.setMapOutputKeyClass(LongWritable.class);
        // job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(source));
        FileOutputFormat.setOutputPath(job, new Path(dest));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}