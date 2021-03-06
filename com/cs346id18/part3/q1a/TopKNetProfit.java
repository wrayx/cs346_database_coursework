package com.cs346id18.part3.q1a;

// importing Libraries
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKNetProfit {
    public static class TopKNetProfitMapper extends
            Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            /** Get start_date and end_date values passed from command lines at runtime */
            long start_date = Long.parseLong(conf.get("start_date"));
            long end_date = Long.parseLong(conf.get("end_date"));

            /** Get data values from the .dat file */
            String[] tokens = value.toString().split(Pattern.quote("|"), -1);
            String sold_date_str = tokens[0];
            String store_str = tokens[7];
            String net_paid_str = tokens[20];
            long sold_date;
            double net_paid;
            int store;

            /** check if each cell is empty */
            try {
                store = Integer.parseInt(store_str.trim());
            } catch (NumberFormatException e) {
                store = -1;
            }

            try {
                sold_date = Long.parseLong(sold_date_str.trim());
            } catch (NumberFormatException e) {
                sold_date = 0;
            }

            try {
                net_paid = Float.parseFloat(net_paid_str.trim());
            } catch (NumberFormatException e) {
                net_paid = 0;
            }

            /** insert data into treeMap,
                we want top K net profit entries
                so we pass net_paid as key */
            if (store != -1 && net_paid != 0 && sold_date != 0 && sold_date >= start_date && sold_date <= end_date) {
                context.write(new IntWritable(store), new DoubleWritable(net_paid));
            }

        }
    }
    public static class TopKNetProfitCombiner extends 
            Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private double totalNetProfit;

        public void reduce( IntWritable key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            int store = key.get();
            totalNetProfit = 0;
            for (DoubleWritable value : values) {
                totalNetProfit += value.get();
            }
            context.write(new IntWritable(store), new DoubleWritable(totalNetProfit));
        }
    }
    public static class TopKNetProfitReducer extends
            Reducer<IntWritable, DoubleWritable, Text, Text> {
        
        /** TreeMap for sorting the total net profit in descending order */
        private TreeMap<Double, Integer> tmap;
        /** variable for storing the total net profit value */
        private double totalNetProfit;

        public void setup(Context context) throws IOException,
                InterruptedException {
            tmap = new TreeMap<Double, Integer>(Comparator.reverseOrder());//sort by descending order
        }

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));

            int store = key.get();
            totalNetProfit = 0;
            for (DoubleWritable value : values) {
                totalNetProfit += value.get();//sum up the net paid
            }
            tmap.put(totalNetProfit, store);//put in tree

            if (tmap.size() > k) {
                tmap.remove(tmap.lastKey());
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Map.Entry<Double, Integer> entry : tmap.entrySet()) {
                //collating data for later use
                //key and value are stored in Text datatype for later print
                DecimalFormat df = new DecimalFormat("#.##");
                totalNetProfit = (double) entry.getKey();
                int store = entry.getValue();
                String totalNetProfit_str = String.valueOf(df.format(totalNetProfit));
                String columnName = "ss_store_sk_";
                columnName = columnName.concat(Integer.toString(store));

                context.write(new Text(columnName), new Text(totalNetProfit_str));
            }
        }

    }

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println(
                    "Usage: Top Net Profit <K> <start_date> <end_date> <input_file> <output_path>");
            System.exit(-1);
        }
        //get parameters
        String k = args[0];
        String start_date = args[1];
        String end_date = args[2];
        String source = args[3];
        String dest = args[4];

        Configuration conf = new Configuration();
        conf.set("K", k);
        conf.set("start_date", start_date);
        conf.set("end_date", end_date);
        //set job
        Job job = Job.getInstance(conf, "TopK");
        job.setJarByClass(TopKNetProfit.class);
        job.setMapperClass(TopKNetProfitMapper.class);
        job.setCombinerClass(TopKNetProfitCombiner.class);
        job.setReducerClass(TopKNetProfitReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(source));
        FileOutputFormat.setOutputPath(job, new Path(dest));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}