
import java.io.IOException;

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

public class SeverityAccidentCount {

    public static class AccidentMapper
            extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

        private final IntWritable severity = new IntWritable();
        private final LongWritable one = new LongWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t", -1);

            // Skip header and malformed records
            if (fields.length < 19 || fields[0].equals("ID")) {
                return;
            }

            // Severity is at index 6
            try {
                int severityValue = Integer.parseInt(fields[6].trim());

                if (severityValue >= 1 && severityValue <= 4) {
                    severity.set(severityValue);
                    context.write(severity, one);
                }

            } catch (NumberFormatException e) {
                // Ignore records with invalid severity values
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
        Job job = Job.getInstance(conf, "Severity Wise Accident Count");

        job.setJarByClass(SeverityAccidentCount.class);

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
