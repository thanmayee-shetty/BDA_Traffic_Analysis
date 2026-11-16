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

public class YearlyAccidentCount {

    public static class AccidentMapper
            extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

        private final IntWritable year = new IntWritable();
        private final LongWritable one = new LongWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t", -1);

            // Skip header and malformed records
            if (fields.length < 19 || fields[0].equals("ID")) {
                return;
            }

            // Year is at index 2
            try {
                int yearValue = Integer.parseInt(fields[2].trim());

                if (yearValue >= 2016 && yearValue <= 2023) {
                    year.set(yearValue);
                    context.write(year, one);
                }

            } catch (NumberFormatException e) {
                // Ignore records with invalid year values
            }
        }
    }

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

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Yearly Accident Count");

        job.setJarByClass(YearlyAccidentCount.class);

        job.setMapperClass(AccidentMapper.class);
        job.setCombinerClass(AccidentReducer.class);
        job.setReducerClass(AccidentReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
