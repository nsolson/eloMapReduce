package cs435.nba.elo;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	private static final String JOB_ONE_OUT_DIR = "JOB_ONE";

	private static final int NUM_JOBS = 2;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 4) {
			System.err.println("Usage: <jar file> <games file> <players file> <tmp_dir> <output dir>");
			System.exit(-1);
		}
		String gamesFile = args[0];
		String playersFile = args[1];
		String tmpDir = args[2];
		String outDir = args[3];

		String jobOneOutputPath = tmpDir + File.separator + JOB_ONE_OUT_DIR;

		/* Job 1 */
		// Input: GamesFile + PlayersFile
		// Output: Games with players on single line
		System.out.println("\n***** Job 1/" + NUM_JOBS + " Starting *****\n");
		Configuration confOne = new Configuration();
		Job jobOne = Job.getInstance(confOne);
		jobOne.setJarByClass(Main.class);
		jobOne.setMapperClass(GamePlayerMapper.class);
		jobOne.setReducerClass(GamePlayerReducer.class);
		jobOne.setMapOutputKeyClass(Text.class);
		jobOne.setMapOutputValueClass(Text.class);
		jobOne.setOutputKeyClass(NullWritable.class);
		jobOne.setOutputValueClass(Text.class);
		jobOne.setInputFormatClass(TextInputFormat.class);
		jobOne.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobOne, new Path(gamesFile), new Path(playersFile));
		FileOutputFormat.setOutputPath(jobOne, new Path(jobOneOutputPath));

		if (!jobOne.waitForCompletion(true)) {
			System.err.println("\nERROR: Job 1 FAILED\n");
			System.exit(1);
		}
		System.out.println("\n***** Job 1/" + NUM_JOBS + " Finished *****\n");
		/* End Job 1 */

		/* Job 2 */
		// Input: Output from Job 1
		// Output: Print of games with teams and players before/after elo
		System.out.println("\n***** Job 2/" + NUM_JOBS + " Starting *****\n");
		Configuration confTwo = new Configuration();
		Job jobTwo = Job.getInstance(confTwo);
		jobTwo.setJarByClass(Main.class);
		jobTwo.setMapperClass(GameEloMapper.class);
		jobTwo.setPartitionerClass(KFactorPartitioner.class);
		jobTwo.setGroupingComparatorClass(KFactorGroupComparator.class);
		jobTwo.setReducerClass(GameEloReducer.class);
		jobTwo.setMapOutputKeyClass(KFactorDateWritable.class);
		jobTwo.setMapOutputValueClass(GameWritable.class);
		jobTwo.setOutputKeyClass(IntWritable.class);
		jobTwo.setOutputValueClass(Text.class);
		jobTwo.setInputFormatClass(TextInputFormat.class);
		jobTwo.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobTwo, new Path(jobOneOutputPath));
		FileOutputFormat.setOutputPath(jobTwo, new Path(outDir));

		if (!jobTwo.waitForCompletion(true)) {
			System.err.println("\nERROR: Job 2 FAILED\n");
			System.exit(2);
		}
		System.out.println("\n***** Job 2/" + NUM_JOBS + " Finished *****\n");
		/* End Job 2 */

		System.exit(0);

	}

}
