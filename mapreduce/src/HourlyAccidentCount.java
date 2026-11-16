
import org.apache.hadoop.io.Text;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HourlyAccidentCount {

    // MAPPER
    public static class AccidentMapper
            extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

        private final IntWritable hourKey = new IntWritable();
        private final LongWritable one = new LongWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Split TSV record
            String[] fields = line.split("\t", -1);

            // Ignore header and malformed records
            if (fields.length < 19 || fields[0].equals("ID")) {
                return;
            }

            try {
                int hour = Integer.parseInt(fields[5]);

                if (hour >= 0 && hour <= 23) {
                    hourKey.set(hour);
                    context.write(hourKey, one);
                }

            } catch (NumberFormatException e) {
                // Ignore records with invalid hour values
            }
        }
    }


    // REDUCER AND COMBINER
    public static class AccidentReducer
            extends Reducer<IntWritable, LongWritable,
                            IntWritable, LongWritable> {

        private final LongWritable result = new LongWritable();

        public void reduce(IntWritable key, Iterable<LongWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            long total = 0;

            for (LongWritable value : values) {
                total += value.get();
            }

            result.set(total);
            context.write(key, result);
        }
    }


    // DRIVER
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: HourlyAccidentCount <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Hourly Accident Analysis");

        job.setJarByClass(HourlyAccidentCount.class);

        job.setMapperClass(AccidentMapper.class);

        // Combiner performs local aggregation
        job.setCombinerClass(AccidentReducer.class);

        job.setReducerClass(AccidentReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // One reducer produces a single sorted output file
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(
            job, new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job, new Path(args[1])
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
