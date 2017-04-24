package cs435.nba.elo;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	private static final String JOB_ONE_OUT_DIR = "GamePlayerPerLine";
	private static final String JOB_TWO_OUT_DIR = "BeforeAfterElo";
	private static final String JOB_THREE_A_OUT_DIR = "BestPlayers";
	private static final String JOB_THREE_B_OUT_DIR = "BestTeams";
	private static final String JOB_THREE_C_OUT_DIR = "BestKFactors";

	private static final int NUM_JOBS = 3;

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
		String jobTwoOutputPath = tmpDir + File.separator + JOB_TWO_OUT_DIR;
		String jobThreeAOutputPath = tmpDir + File.separator + JOB_THREE_A_OUT_DIR;
		String jobThreeBOutputPath = tmpDir + File.separator + JOB_THREE_B_OUT_DIR;
		String jobThreeCOutputPath = tmpDir + File.separator + JOB_THREE_C_OUT_DIR;

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
		jobTwo.setOutputKeyClass(DoubleWritable.class);
		jobTwo.setOutputValueClass(Text.class);
		jobTwo.setInputFormatClass(TextInputFormat.class);
		jobTwo.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobTwo, new Path(jobOneOutputPath));
		FileOutputFormat.setOutputPath(jobTwo, new Path(jobTwoOutputPath));

		if (!jobTwo.waitForCompletion(true)) {
			System.err.println("\nERROR: Job 2 FAILED\n");
			System.exit(2);
		}
		System.out.println("\n***** Job 2/" + NUM_JOBS + " Finished *****\n");
		/* End Job 2 */

		/* Job 3 */
		// Group of jobs
		System.out.println("\n***** Job 3/" + NUM_JOBS + " Starting *****\n");
		JobGroup jobGroupThree = new JobGroup("Job 3");

		/* Job 3a */
		// Best Players
		// Input: Output from Job 2
		// Output: Top 10 players for each k Value
		Configuration confThreeA = new Configuration();
		Job jobThreeA = Job.getInstance(confThreeA);
		jobThreeA.setJarByClass(Main.class);
		jobThreeA.setMapperClass(KFactorBestPlayerMapper.class);
		jobThreeA.setCombinerClass(KFactorBestCombiner.class);
		jobThreeA.setReducerClass(KFactorBestReducer.class);
		jobThreeA.setMapOutputKeyClass(DoubleWritable.class);
		jobThreeA.setMapOutputValueClass(IdEloWritable.class);
		jobThreeA.setOutputKeyClass(DoubleWritable.class);
		jobThreeA.setOutputValueClass(Text.class);
		jobThreeA.setInputFormatClass(TextInputFormat.class);
		jobThreeA.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeA, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeA, new Path(jobThreeAOutputPath));
		jobGroupThree.addJob(jobThreeA, "Best Player");
		/* End Job 3a */

		/* Job 3b */
		// Best Teams
		// Input: Output from Job 2
		// Output: Top 10 teams for each K value
		Configuration confThreeB = new Configuration();
		Job jobThreeB = Job.getInstance(confThreeB);
		jobThreeB.setJarByClass(Main.class);
		jobThreeB.setMapperClass(KFactorBestTeamMapper.class);
		jobThreeB.setCombinerClass(KFactorBestCombiner.class);
		jobThreeB.setReducerClass(KFactorBestReducer.class);
		jobThreeB.setMapOutputKeyClass(DoubleWritable.class);
		jobThreeB.setMapOutputValueClass(IdEloWritable.class);
		jobThreeB.setOutputKeyClass(DoubleWritable.class);
		jobThreeB.setOutputValueClass(Text.class);
		jobThreeB.setInputFormatClass(TextInputFormat.class);
		jobThreeB.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeB, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeB, new Path(jobThreeBOutputPath));
		jobGroupThree.addJob(jobThreeB, "Best Team");
		/* End Job 3b */

		/* Job 3c */
		// K Factor accuracy
		// Input: Output from Job 2
		// Output: KFactor %Correct
		Configuration confThreeC = new Configuration();
		Job jobThreeC = Job.getInstance(confThreeC);
		jobThreeC.setJarByClass(Main.class);
		jobThreeC.setMapperClass(KFactorAccuracyMapper.class);
		jobThreeC.setReducerClass(KFactorAccuracyReducer.class);
		jobThreeC.setMapOutputKeyClass(DoubleWritable.class);
		jobThreeC.setMapOutputValueClass(IntWritable.class);
		jobThreeC.setOutputKeyClass(DoubleWritable.class);
		jobThreeC.setOutputValueClass(DoubleWritable.class);
		jobThreeC.setInputFormatClass(TextInputFormat.class);
		jobThreeC.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeC, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeC, new Path(jobThreeCOutputPath));
		jobGroupThree.addJob(jobThreeC, "K Factor Accuracy");
		/* End Job 3c */

		jobGroupThree.runAndWait();
		if (!jobGroupThree.isSuccessful()) {
			System.err.println("ERROR: Job 3 FAILED");
			System.exit(3);
		}
		System.out.println("\n***** Job 3/" + NUM_JOBS + " Finished *****\n");
		/* End Job 3 */

		System.exit(0);

	}

}
