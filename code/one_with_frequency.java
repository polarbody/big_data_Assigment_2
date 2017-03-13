package Lishi.mdp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class one_with_frequency extends Configured implements Tool {

	private static final Logger LOG = Logger
			.getLogger(one_with_frequency.class);

	public static enum COUNTER {
		COUNTERLINE,
	};

	public static void main(String[] args) throws Exception {
        system.out.println(Arrays.toString(args))
		int res = ToolRunner.run(new one_with_frequency(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.Job(getConf(), "one_with_frequency");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.getConfiguration().set("separator", ", ");
        job.setNumReduceTasks(1);


		for (int i = 0; i < args.length; i += 1) {
			if ("-skip".equals(args[i])) {
				job.getConfiguration().setBoolean("skip.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
				LOG.info("Added: " + args[i]);
			}
		}

		
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);
		long counter = job.getCounters().findCounter(COUNTER.COUNTERLINE)
				.getValue();
		Path outFile = new Path("COUNTERLINE.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		private Set<String> patternsToSkip = new HashSet<String>();
		private BufferedReader fis;

		protected void setup(Mapper.Context context) throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			if (config.getBoolean("skip.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parseSkipFile(localPaths[0]);
			}
		}

		private void parseSkipFile(URI patternsURI) {
			LOG.info("Added:" + patternsURI);
			try {
				fis = new BufferedReader(new FileReader(new File(
						patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the cached file '"+ patternsURI+ "' : "+ StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			for (String word : value.toString().split("\\s*\\b\\s*")) {

				Pattern p = Pattern.compile("[^A-Za-z0-9]");

				if (value.toString().length() == 0
						|| word.toLowerCase().isEmpty()
						|| patternsToSkip.contains(word.toLowerCase())
						|| p.matcher(word.toLowerCase()).find()) {
					continue;
				}

				context.write(key, new Text(word.toLowerCase()));
			}
		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

		private BufferedReader reader;

		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			ArrayList<String> wordsL = new ArrayList<String>();

			HashMap<String, String> WordCount = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File("/home/cloudera/workspace/Lishi/output/WordCount.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] word = pattern.split(",");
				WordCount.put(word[0], word[1]);
			}

			for (Text word : values) {
				wordsL.add(word.toString());
			}

			HashSet<String> wordsHS = new HashSet<String>(wordsL);

			StringBuilder countedword = new StringBuilder();

			String firstprefix = "";
			for (String word : wordsHS) {
				countedword.append(firstprefix);
				firstprefix = ", ";
				countedword.append(word + "#" + WordCount.get(word));
			}

			java.util.List<String> countedword_xx= Arrays
					.asList(countedword.toString().split("\\s*,\\s*"));

			Collections.sort(countedword_xx, new Comparator<String>() {
				public int compare(String o1, String o2) {
					return extractInt(o1) - extractInt(o2);
				}

				int extractInt(String s) {
					String num = s.replaceAll("[^#\\d+]", "");
					num = num.replaceAll("\\d+#", "");
					num = num.replaceAll("#", "");
					return num.isEmpty() ? 0 : Integer.parseInt(num);
				}
			});

			StringBuilder sortedword = new StringBuilder();

			String secondprefix = "";
			for (String word : countedword_xx) {
				sortedword.append(secondprefix);
				secondprefix = ", ";
				sortedword.append(word);
			}

			context.getCounter(COUNTER.COUNTERLINE).increment(1);

			context.write(key, new Text(sortedword.toString()));

		}
	}
}
