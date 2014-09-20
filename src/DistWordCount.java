/* Original code from http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
 * Modified By: Manish Sapkota
 * Modified to read the pattern file separated with space and count the words matching the pattern
 */
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class DistWordCount extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		static enum Counters { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;
		private String inputFile;

		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");

			Path[] patternsFiles = new Path[0];
			try {
				patternsFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			}
			for (Path patternsFile : patternsFiles) {
				parseMatchFile(patternsFile);
			}
		}

		private void parseMatchFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				
				// create the pattern file has array for matching from the distributed cache
				while ((pattern = fis.readLine()) != null) {
					StringTokenizer tokenizer = new StringTokenizer(pattern);
					while (tokenizer.hasMoreTokens()) {
						patternsToSkip.add(tokenizer.nextToken());
					}
				}
				fis.close();
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			String currentString="";
			while (tokenizer.hasMoreTokens()) {
				currentString=tokenizer.nextToken();
				//  check and see if the current word is in the pattern file else ignore
				if(patternsToSkip.contains(currentString)){
					word.set(currentString);
					output.collect(word, one);
					reporter.incrCounter(Counters.INPUT_WORDS, 1);
				}
			}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
	
		JobConf conf = new JobConf(getConf(), DistWordCount.class);
		conf.setJobName("distwordcount");
		conf.setJarByClass(DistWordCount.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DistWordCount(), args);
		System.exit(res);
	}
}