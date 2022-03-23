// importing Libraries
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKItems {
    public static class TopKItemsMapper extends
            Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            // we will use the value passed in start date and end date at runtime
            long start_date = Long.parseLong(conf.get("start_date"));
            long end_date = Long.parseLong(conf.get("end_date"));

            String[] tokens = value.toString().split(Pattern.quote("|"), -1);
            String sold_date_str = tokens[0];
            String item_sk_str = tokens[2];
            String sold_quantity_str = tokens[10];
            long sold_date;
            double sold_quantity;
            int item_id;

            // check if the cell is empty
            try {
                item_id = Integer.parseInt(item_sk_str.trim());
            } catch (NumberFormatException e) {
                item_id = -1;
            }

            try {
                sold_date = Long.parseLong(sold_date_str.trim());
            } catch (NumberFormatException e) {
                sold_date = 0;
            }

            try {
                sold_quantity = Double.parseDouble(sold_quantity_str.trim());
            } catch (NumberFormatException e) {
                sold_quantity = 0;
            }

            // insert data into treeMap,
            // we want top K item entries
            // so we pass quantity as key
            if (item_id != -1 && sold_quantity != 0 && sold_date != 0 && sold_date >= start_date
                    && sold_date <= end_date) {
                context.write(new IntWritable(item_id), new DoubleWritable(sold_quantity));
            }

        }
    }
    /**combiner phase, sum up the net paid of each row of each map task*/
    public static class TopKItemsCombiner extends 
            Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private double total_quantity;

        public void reduce( IntWritable key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            int store = key.get();
            total_quantity = 0;
            for (DoubleWritable value : values) {
                total_quantity += value.get();//sum up the quantity
            }
            context.write(new IntWritable(store), new DoubleWritable(total_quantity));
        }
    }
    /**reducer phase,using tree map to sort the data*/
    public static class TopKItemsReducer extends
            Reducer<IntWritable, DoubleWritable, Text, Text> {

        private TreeMap<Integer, Integer> tmap2;//treemap
        private int total_sold_quantity;

        public void setup(Context context) throws IOException,
                InterruptedException {
            tmap2 = new TreeMap<Integer, Integer>(Comparator.reverseOrder());//sort by descending order
        }

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));
            int item_id = key.get();

            total_sold_quantity = 0;
            for (DoubleWritable value : values) {
                total_sold_quantity += value.get();//sum up the quantities
            }
            tmap2.put(total_sold_quantity, item_id);//put in tree

            if (tmap2.size() > k) {
                tmap2.remove(tmap2.lastKey());
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Map.Entry<Integer, Integer> entry : tmap2.entrySet()) {
                //collating data for later use
                //key and value are stored in Text datatype for later print
                total_sold_quantity = entry.getKey();
                int item_id = entry.getValue();
                String total_sales= String.valueOf(total_sold_quantity);
                String columnName = "ss_item_sk_";
                columnName = columnName.concat(Integer.toString(item_id));

                context.write(new Text(columnName), new Text(total_sales));
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
        job.setJarByClass(TopKItems.class);
        job.setMapperClass(TopKItemsMapper.class);
        job.setCombinerClass(TopKItemsCombiner.class);
        job.setReducerClass(TopKItemsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(source));
        FileOutputFormat.setOutputPath(job, new Path(dest));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}