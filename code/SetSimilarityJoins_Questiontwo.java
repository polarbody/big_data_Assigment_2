package Lishi.mdp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SetSimilarityJoinsB extends Configured implements Tool {

	public static enum COUNTER {
		COUNTER_COMPARISIONS_TWO,
	};

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new SetSimilarityJoinsB(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = Job.getInstance(getConf(), "SetSimilarityJoins_Questiontwo");

		job.setJarByClass(SetSimilarityJoinsB.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ", ");
		job.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {fs.delete(new Path(args[1]), true);}

		job.waitForCompletion(true);
		long counter = job.getCounters().findCounter(COUNTER.NB_COMPARISIONS_TWO).getValue();
		Path outFile = new Path("COUNTER_COMPARISIONS_two.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private Text word = new Text();
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordsLong = value.toString().split(" ");
			long numkeepedwords = Math.round(wordsLong.length- (wordsLong.length * 0.8) + 1);
			String[] keepedwordsL = Arrays.copyOfRange(wordsLong, 0,(int) numkeepedwords);

			for (String keepedwords : keepedwordsL) {
				word.set(keepedwords);
				context.write(word, key);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private BufferedReader reader;

		public double jaccard(TreeSet<String> s1, TreeSet<String> s2) {

			if (s1.size() < s2.size()) {
				TreeSet<String> jac = s1;
				jac.retainAll(s2);
				int inter = jac.size();
				s1.addAll(s2);
				int union = s1.size();
				return (double) inter / union;
			} else {
				TreeSet<String> jac = s2;
				jac.retainAll(s1);
				int inter = jac.size();
				s2.addAll(s1);
				int union = s2.size();
				return (double) inter / union;
			}

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

                    HashMap<String, String> linesLS = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File("/home/cloudera/workspace/Lishi/output/two_without_frequency.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {String[] line = pattern.split(",");linesLS.put(line], line[1]);}

			ArrayList<String> wordsLong = new ArrayList<String>();

			for (Text word : values) {
				wordsLong.add(word.toString());
			}

			if (wordsLong.size() > 1) {
				ArrayList<String> pairs = new ArrayList<String>();
				for (int i = 0; i < wordsLong.size(); ++i) {
					for (int j = i + 1; j < wordsLong.size(); ++j) {
						String pair = new String(wordsLong.get(i) + " "
								+ wordsLong.get(j));
						pairs.add(pair);
					}
				}

				for (String pair : pairs) {
					TreeSet<String> pair1st = new TreeSet<String>();
					String pair1stS = linesLS.get(pair.split(" ")[0].toString());
					for (String word : pair1stS.split(" ")) {
						pair1st.add(word);
					}

					TreeSet<String> pair2nd = new TreeSet<String>();
					String pair2ndS = linesLS.get(pair.split(" ")[1].toString());
					for (String word : pair2ndS.split(" ")) {
						pair2nd.add(word);
					}

					context.getCounter(COUNTER.COUNTER_COMPARISIONS_TWO)
							.increment(1);
					double sim = jaccard(pair1st,
							pair2nd);

					if (sim >= 0.8) {
                        context.write(new Text("(" + pair.split(" ")[0] + ", "+ pair.split(" ")[1] + ")"),
								new Text(String.valueOf(sim)));
					}
				}
			}
		}
	}
}
