
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeekdayWeekendCount {

    public static class AccidentMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text category = new Text();
        private final LongWritable one = new LongWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t", -1);

            // Skip header and malformed records
            if (fields.length < 19 || fields[0].equals("ID")) {
                return;
            }

            // DayOfWeek is at index 4
            String day = fields[4].trim();

            if (day.equalsIgnoreCase("Saturday") ||
                day.equalsIgnoreCase("Sunday")) {

                category.set("Weekend");
                context.write(category, one);

            } else if (
                day.equalsIgnoreCase("Monday") ||
                day.equalsIgnoreCase("Tuesday") ||
                day.equalsIgnoreCase("Wednesday") ||
                day.equalsIgnoreCase("Thursday") ||
                day.equalsIgnoreCase("Friday")) {

                category.set("Weekday");
                context.write(category, one);
            }
        }
    }

    public static class AccidentReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
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
        Job job = Job.getInstance(conf, "Weekday Weekend Accident Count");

        job.setJarByClass(WeekdayWeekendCount.class);

        job.setMapperClass(AccidentMapper.class);
        job.setCombinerClass(AccidentReducer.class);
        job.setReducerClass(AccidentReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
