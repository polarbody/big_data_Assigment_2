package Lishi.mdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;

public class Pairing implements WritableComparable<Pairing> {

	private Text first;
	private Text second;

	public Pairing(Text first, Text second) {
		set(first, second);
	}

	public Pairing() {
		set(new Text(), new Text());
	}

	public Pairing(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public Text FFirst() {
		return first;
	}

	public Text SSecond() {
		return second;
	}

	public void set(Text FFirst, Text SSecond) {
		this.first = first;
		this.second = second;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
    
	@Override
	public String SString() {
		return first + " " + second;
	}

	@Override
	public int compareTo(Pairing other) {
		int cmpFirstFirst = first.compareTo(other.first);
		int cmpSecondSecond = second.compareTo(other.second);
		int cmpFirstSecond = first.compareTo(other.second);
		int cmpSecondFirst = second.compareTo(other.first);

		if (cmpFirstFirst == 0 && cmpSecondSecond == 0 || cmpFirstSecond == 0
				&& cmpSecondFirst == 0) {
			return 0;
		}

		Text thisSmaller;
		Text thatSmaller;

		Text thisBigger;
		Text thatBigger;

		if (this.first.compareTo(this.second) < 0) {
			thisSmaller = this.first;
			thisBigger = this.second;
		} else {
			thisSmaller = this.second;
			thisBigger = this.first;
		}

		if (other.first.compareTo(other.second) < 0) {
			otherSmaller = other.first;
			otherBigger = other.second;
		} else {
			otherSmaller = other.second;
			otherBigger = other.first;
		}

		int cmpThisSmallerOtherSmaller = thisSmaller.compareTo(otherSmaller);
		int cmpThisBiggerOtherBigger = thisBigger.compareTo(otherBigger);

		if (cmpThisSmallerOtherSmaller == 0) {
			return cmpThisBiggerOtherBigger;
		} else {
			return cmpThisSmallerOtherSmaller;
		}
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

}

public class SetSimilarityJoins_one extends Configured implements
		Tool {

	public static enum COUNTER {
		COUNTER_COMPARISIONS_one,
	};

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new SetSimilarityJoins_one(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = Job.getInstance(getConf(), "SetSimilarityJoins_one");

		job.setJarByClass(SetSimilarityJoins_one.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				",");
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ", ");
		job.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);
		long counter = job.getCounters()
				.findCounter(COUNTER.COUNTER_COMPARISIONS_ONE).getValue();
		Path outFile = new Path("COUNTER_COMPARISIONS_ONE.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<Text, Text, TextPair, Text> {

		private BufferedReader reader;
		private static  Pairing Pairing = new Pairing();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			HashMap<String, String> linesHP = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/Lishi/output/two_without_frequency.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] line = pattern.split(",");
				linesHP.put(line[0], line[1]);
			}

			for (String line : linesHP.keySet()) {
				if (key.toString().equals(line)) {
					continue;text
				}

				Pairing.set(key, new Text(line));
				context.write(Pairing, new Text(value.toString()));
			}
		}
	}

	public static class Reduce extends Reducer<TextPair, Text, Text, Text> {

		private BufferedReader reader;

		public double jaccard(TreeSet<String> s1, TreeSet<String> s2) {
            
            if (s1.equals(s2)) {
                return 1;
            }
            
            Map<String, Integer> profile1 = getProfile(s1);
            Map<String, Integer> profile2 = getProfile(s2);
            
            Set<String> union = new HashSet<String>();
            union.addAll(profile1.keySet());
            union.addAll(profile2.keySet());
            
            
            for (String key : union) {
                if (profile1.containsKey(key) && profile2.containsKey(key)) {
                    inter++;
                }
            }
            
            return 1.0 * inter / union.size();
        }

		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, String> linesHP = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File("/home/cloudera/workspace/Lishi/output/two_without_frequency.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] line = pattern.split(",");
				linesHP.put(line[0], line[1]);
			}

			TreeSet<String> wordsoflinetreeset = new TreeSet<String>();
			String wordsoflinetreeset = linesHP.get(key.getSecond()
					.toString());
			for (String word : wordsoflinetreeset.split(" ")) {
				wordsofline.add(word);
			}

			TreeSet<String> wordstreeset = new TreeSet<String>();

			for (String word : values.iterator().next().toString().split(" ")) {
				wordsTS.add(word);
			}

			context.getCounter(COUNTER.COUNTER_COMPARISIONS_ONE).increment(1);
			double sim = jaccard(wordtreeset, wordsoflinetreeset);

			if (sim >= 0.8) {
				context.write(new Text("(" + key.getFirst() + ", " + key.getSecond() + ")"),
						new Text(String.valueOf(sim)));
			}
		}
	}
}
