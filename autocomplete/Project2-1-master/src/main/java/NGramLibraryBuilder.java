
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// input keyvalue, input的形式， output， output的形式
		// IntWritable is a Writable class that wraps a java int.

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// 拆分到2gram到n gram
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//inputKey: offset
			//inputValue: line -> sentence
			//read sentence -> split into 2gram-ngram
			//write to disk
			//outputKey = gram
			//outputValue = 1

			//split sentence into words
			//i love big data n=3
			//i love, i love big
			//love big, love big data
			//big data
			String sentence = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
			String[] words = sentence.split("\\s+"); // 根据space来拆分
			if (words.length < 2) {
				return;
			}

			StringBuilder phrase;
			for (int i = 0; i < words.length; i++) {
				phrase = new StringBuilder();
				phrase.append(words[i]);
				for (int j = 1; i + j < words.length && j < noGram; j++) {
					phrase.append(" ");
					phrase.append(words[i+j]);
					context.write(new Text(phrase.toString().trim()), new IntWritable(1)); // 写出
				}
			}

		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//input key = ngram big data
			//input value = <1, 1, 1, 1....> Interator
			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum)); // 写出到hdfs， context是一个媒介
			// bigdata\t123
		}
	}
}