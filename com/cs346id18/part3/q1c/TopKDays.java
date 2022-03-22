package com.cs346id18.part3.q1c;

// importing Libraries
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKDays {
    public static class TopKDaysMapper extends
            Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            // we will use the value passed in start date and end date at runtime
            long start_date = Long.parseLong(conf.get("start_date"));
            long end_date = Long.parseLong(conf.get("end_date"));

            String[] tokens = value.toString().split(Pattern.quote("|"), -1);
            String sold_date_str = tokens[0];
            String net_paid_inc_str = tokens[21];

            long sold_date;
            double net_paid_inc;

            // check if the cell is empty

            try {
                sold_date = Long.parseLong(sold_date_str.trim());
            } catch (NumberFormatException e) {
                sold_date = 0;
            }

            try {
                net_paid_inc = Double.parseDouble(net_paid_inc_str.trim());
            } catch (NumberFormatException e) {
                net_paid_inc = 0;
            }

            // insert data into treeMap,
            // we want top K net profit entries
            // so we pass net_paid as key
            if (net_paid_inc != 0 && sold_date != 0 && sold_date > start_date && sold_date < end_date) {
                context.write(new LongWritable(sold_date), new DoubleWritable(net_paid_inc));
            }

        }
    }

    public static class TopKDaysCombiner extends 
            Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

        private double totalNetPaidIncTax;

        public void reduce( LongWritable key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            long sold_date = key.get();
            totalNetPaidIncTax = 0;
            for (DoubleWritable value : values) {
                totalNetPaidIncTax += value.get();
            }
            context.write(new LongWritable(sold_date), new DoubleWritable(totalNetPaidIncTax));
        }
    }

    public static class TopKDaysReducer extends
            Reducer<LongWritable, DoubleWritable, Text, Text> {

        private TreeMap<Double, Long> tmap2;
        private double total_net_paid_inc;

        public void setup(Context context) throws IOException,
                InterruptedException {
            tmap2 = new TreeMap<Double, Long>(Comparator.reverseOrder());
        }

        @Override
        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));

            long sold_date = key.get();
            total_net_paid_inc = 0;
            for (DoubleWritable value : values) {
                total_net_paid_inc += value.get();
            }
            tmap2.put(total_net_paid_inc, sold_date);

            if (tmap2.size() > k) {
                tmap2.remove(tmap2.lastKey());
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Map.Entry<Double, Long> entry : tmap2.entrySet()) {
                DecimalFormat df = new DecimalFormat("#.##");
                total_net_paid_inc = entry.getKey();
                long sold_date = entry.getValue();
                String total_net_paid_inc_ss_str = String.valueOf(df.format(total_net_paid_inc));
                String columnName = "ss_sold_data_sk_";
                columnName = columnName.concat(Long.toString(sold_date));

                context.write(new Text(columnName), new Text(total_net_paid_inc_ss_str));
            }
        }

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
        job.setJarByClass(TopKDays.class);
        job.setMapperClass(TopKDaysMapper.class);
        job.setCombinerClass(TopKDaysCombiner.class);
        job.setReducerClass(TopKDaysReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(source));
        FileOutputFormat.setOutputPath(job, new Path(dest));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}