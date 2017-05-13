//Collect frequency of all the words who have a length more than some specified value
package mapreduce.advanced.demo.configurations;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StaticConfigurations {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("min.word.length", "3");
		
		Job job = new Job(conf, "Static Configuration");
		job.setJarByClass(StaticConfigurations.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(SCMapper.class);
		job.setReducerClass(SCReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
				
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out, true);
		
		job.waitForCompletion(true);
	}
	
	public static class SCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		Text outKey = new Text();
		IntWritable outValue = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String word : words) {
				if (word.length() < Integer.parseInt(context.getConfiguration().get("min.word.length"))) {
					continue;
				}
				outKey.set(word);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class SCReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable outValue = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}

}
