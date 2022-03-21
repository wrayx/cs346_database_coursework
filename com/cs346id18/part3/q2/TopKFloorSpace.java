package com.cs346id18.part3.q2;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKFloorSpace {
    public static class StoreSalesDataMapper extends
            Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            // we will use the value passed in start date and end date at runtime
            long start_date = Long.parseLong(conf.get("start_date"));
            long end_date = Long.parseLong(conf.get("end_date"));

            String[] tokens = value.toString().split(Pattern.quote("|"), -1);
            String sold_date_str = tokens[0];
            String store_str = tokens[7];
            String totalNetProfit_str = tokens[20];
            long sold_date;
            float net_paid;
            int store;

            // check if the cell is empty
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
                net_paid = Float.parseFloat(totalNetProfit_str.trim());
            } catch (NumberFormatException e) {
                net_paid = 0;
            }
            
            // insert data into treeMap,
            // we want top K net profit entries
            // so we pass net_paid as key
            if (store != -1 && net_paid != 0 && sold_date != 0 && sold_date > start_date && sold_date < end_date) {
                context.write(new IntWritable(store), new Text(totalNetProfit_str + "\tfs"));
            }
        }
    }
    
    public static class StoreDataMapper extends
            Mapper<LongWritable, Text, IntWritable, Text> {

        // private TreeMap<Float, Integer> tmap;

        // @Override
        // public void setup(Context context) throws IOException,
        //         InterruptedException {
        //     tmap = new TreeMap<Float, Integer>((Comparator.reverseOrder()));
        // }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split(Pattern.quote("|"), -1);
            // for (String t: tokens){
            // System.out.print(t);
            // System.out.print("===");
            // }
            // System.out.println();
            // long sold_date = Long.parseLong(tokens[0].trim());
            String store_str = tokens[0];
            String floor_space_str = tokens[7];
            int store;
            // int floor_space;

            // check if the cell is empty
            try {
                store = Integer.parseInt(store_str.trim());
            } catch (NumberFormatException e) {
                store = -1;
            }
            
            if (store != -1){
                context.write(new IntWritable(store), new Text("np\t" + floor_space_str));
            }

        }
    }

    public static class TopKFloorSpaceReducer extends
            Reducer<IntWritable, Text, Text, Text> {

        // private DoubleWritable result = new DoubleWritable();

        private TreeMap<String, Integer> tmap2;
        private float totalNetProfit;

        public void setup(Context context) throws IOException,
                InterruptedException {
            tmap2 = new TreeMap<String, Integer>(
                new Comparator<String>() {
                    @Override
                    public int compare(String e1, String e2) {
                            String [] inputs_e1 = e1.split("\t");
                            int floorspace_e1;
                            float net_paid_e1;
    
                            try {
                                net_paid_e1 = Float.parseFloat(inputs_e1[0].trim());
                            } catch (NumberFormatException e) {
                                net_paid_e1 = -1;
                            }

                            try {
                                floorspace_e1 = Integer.parseInt(inputs_e1[1].trim());
                            } catch (NumberFormatException e) {
                                floorspace_e1 = -1;
                            }
                            
                            String [] inputs_e2 = e2.split("\t");
                            int floorspace_e2;
                            float net_paid_e2;
    
                            try {
                                net_paid_e2 = Float.parseFloat(inputs_e2[0].trim());
                            } catch (NumberFormatException e) {
                                net_paid_e2 = -1;
                            }
                            try {
                                floorspace_e2 = Integer.parseInt(inputs_e2[1].trim());
                            } catch (NumberFormatException e) {
                                floorspace_e2 = -1;
                            }
                            
                            if (floorspace_e1 == floorspace_e2){
                                return (-1) * Float.compare(net_paid_e1, net_paid_e2);
                            }
                            else {
                                return (-1) * Integer.compare(floorspace_e1, floorspace_e2);
                            }
                    }
                });

            
        }

        // @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));

            int store = key.get();
            double totalNetProfit = 0;
            int floorspace = 0;
            for (Text value : values) {
                String [] parts = value.toString().split("\t");
                if (parts[0].equals("np")){
                    floorspace += Integer.parseInt(parts[1]);
                }
                else if (parts[1].equals("fs")) {
                    totalNetProfit += Double.parseDouble(parts[0]);
                }
                else {
                    totalNetProfit += Double.parseDouble(parts[0]);
                    floorspace += Integer.parseInt(parts[1]);
                }
            }
            DecimalFormat df = new DecimalFormat("#.##");
            df.setMinimumFractionDigits(2);
            df.setMinimumIntegerDigits(9);  
            String totalNetProfit_str = String.valueOf(df.format(totalNetProfit));

            String s =(new StringBuilder()).append(totalNetProfit_str).append('\t').append(Integer.toString(floorspace)).toString();  


            tmap2.put(s, store);

            if (tmap2.size() > k) {
                tmap2.remove(tmap2.lastKey());
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Map.Entry<String, Integer> entry : tmap2.entrySet()) {
                // DecimalFormat df = new DecimalFormat("#.##");
                String s = entry.getKey();
                int store = entry.getValue();
                String columnName = "ss_store_sk_";
                columnName = columnName.concat(Integer.toString(store));

                context.write(new Text(columnName), new Text(s));
            }

        }

    }

    public static void main(String[] args) throws Exception {

        if (args.length != 6) {
            System.err.println(
                    "Usage: Top Net Profit <K> <start_date> <end_date> <input_file_1> <input_file_2> <output_path>");
            System.exit(-1);
        }

        String k = args[0];
        String start_date = args[1];
        String end_date = args[2];
        String inputFile1 = args[3];
        String inputFile2 = args[4];
        String outputDirectory = args[5];

        Configuration conf = new Configuration();
        conf.set("K", k);
        conf.set("start_date", start_date);
        conf.set("end_date", end_date);

        Job job = Job.getInstance(conf, "TopK");
        job.setJarByClass(TopKFloorSpace.class);
        // job.setCombinerClass(TopKFloorSpaceReducer.class);
        job.setReducerClass(TopKFloorSpaceReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputFile1), TextInputFormat.class, StoreSalesDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputFile2), TextInputFormat.class, StoreDataMapper.class);

        // FileInputFormat.addInputPath(job, new Path(source));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}