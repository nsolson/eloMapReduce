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
	private static final String JOB_THREE_C_OUT_DIR = "KFactorPercent";
	private static final String JOB_THREE_D_OUT_DIR = "KFactorErrorSquare";
	private static final String JOB_THREE_E_OUT_DIR = "KFactorError";
	private static final String JOB_THREE_F_OUT_DIR = "KFactorTrueError";

	private static final String JOB_FOUR_A_OUT_DIR = "KFactorPercentRanked";
	private static final String JOB_FOUR_B_OUT_DIR = "KFactorErrorSquareRanked";
	private static final String JOB_FOUR_C_OUT_DIR = "KFactorErrorRanked";
	private static final String JOB_FOUR_D_OUT_DIR = "KFactorTrueErrorRanked";

	private static final int NUM_JOBS = 4;

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
		String jobThreeDOutputPath = tmpDir + File.separator + JOB_THREE_D_OUT_DIR;
		String jobThreeEOutputPath = tmpDir + File.separator + JOB_THREE_E_OUT_DIR;
		String jobThreeFOutputPath = tmpDir + File.separator + JOB_THREE_F_OUT_DIR;
		String jobFourAOutputPath = tmpDir + File.separator + JOB_FOUR_A_OUT_DIR;
		String jobFourBOutputPath = tmpDir + File.separator + JOB_FOUR_B_OUT_DIR;
		String jobFourCOutputPath = tmpDir + File.separator + JOB_FOUR_C_OUT_DIR;
		String jobFourDOutputPath = tmpDir + File.separator + JOB_FOUR_D_OUT_DIR;

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

		/* Job 3a and 3b are for testing */
		if (Constants.TEST_RUN) {

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
		}

		/* Job 3c */
		// K Factor accuracy
		// Input: Output from Job 2
		// Output: KFactor %Correct
		Configuration confThreeC = new Configuration();
		Job jobThreeC = Job.getInstance(confThreeC);
		jobThreeC.setJarByClass(Main.class);
		jobThreeC.setMapperClass(KFactorAccuracyMapper.class);
		jobThreeC.setReducerClass(KFactorAccuracyReducer.class);
		jobThreeC.setMapOutputKeyClass(Text.class);
		jobThreeC.setMapOutputValueClass(IntWritable.class);
		jobThreeC.setOutputKeyClass(Text.class);
		jobThreeC.setOutputValueClass(DoubleWritable.class);
		jobThreeC.setInputFormatClass(TextInputFormat.class);
		jobThreeC.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeC, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeC, new Path(jobThreeCOutputPath));
		jobGroupThree.addJob(jobThreeC, "K Factor Accuracy");
		/* End Job 3c */

		/* Job 3d */
		// K Factor Square Error
		// Input: Output from Job 2
		// Output: KFactor SquareError
		Configuration confThreeD = new Configuration();
		Job jobThreeD = Job.getInstance(confThreeD);
		jobThreeD.setJarByClass(Main.class);
		jobThreeD.setMapperClass(KFactorErrorSquareMapper.class);
		jobThreeD.setReducerClass(KFactorErrorReducer.class);
		jobThreeD.setMapOutputKeyClass(Text.class);
		jobThreeD.setMapOutputValueClass(DoubleWritable.class);
		jobThreeD.setOutputKeyClass(Text.class);
		jobThreeD.setOutputValueClass(DoubleWritable.class);
		jobThreeD.setInputFormatClass(TextInputFormat.class);
		jobThreeD.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeD, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeD, new Path(jobThreeDOutputPath));
		jobGroupThree.addJob(jobThreeD, "K Factor Error Square");
		/* End Job 3d */

		/* Job 3e */
		// K Factor Error
		// Input: Output from Job 2
		// Output: KFactor Error
		Configuration confThreeE = new Configuration();
		Job jobThreeE = Job.getInstance(confThreeE);
		jobThreeE.setJarByClass(Main.class);
		jobThreeE.setMapperClass(KFactorErrorMapper.class);
		jobThreeE.setReducerClass(KFactorErrorReducer.class);
		jobThreeE.setMapOutputKeyClass(Text.class);
		jobThreeE.setMapOutputValueClass(DoubleWritable.class);
		jobThreeE.setOutputKeyClass(Text.class);
		jobThreeE.setOutputValueClass(DoubleWritable.class);
		jobThreeE.setInputFormatClass(TextInputFormat.class);
		jobThreeE.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeE, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeE, new Path(jobThreeEOutputPath));
		jobGroupThree.addJob(jobThreeE, "K Factor Error");
		/* End Job 3e */

		// K Factor True Error
		// Input: Output from Job 2
		// Output: KFactor True Error
		Configuration confThreeF = new Configuration();
		Job jobThreeF = Job.getInstance(confThreeF);
		jobThreeF.setJarByClass(Main.class);
		jobThreeF.setMapperClass(KFactorTrueErrorMapper.class);
		jobThreeF.setReducerClass(KFactorErrorReducer.class);
		jobThreeF.setMapOutputKeyClass(Text.class);
		jobThreeF.setMapOutputValueClass(DoubleWritable.class);
		jobThreeF.setOutputKeyClass(Text.class);
		jobThreeF.setOutputValueClass(DoubleWritable.class);
		jobThreeF.setInputFormatClass(TextInputFormat.class);
		jobThreeF.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobThreeF, new Path(jobTwoOutputPath));
		FileOutputFormat.setOutputPath(jobThreeF, new Path(jobThreeFOutputPath));
		jobGroupThree.addJob(jobThreeF, "K Factor True Error");
		/* End Job 3f */

		jobGroupThree.runAndWait();
		if (!jobGroupThree.isSuccessful()) {
			System.err.println("\nERROR: Job 3 FAILED\n");
			System.exit(3);
		}
		System.out.println("\n***** Job 3/" + NUM_JOBS + " Finished *****\n");
		/* End Job 3 */

		/* Job 4 */
		System.out.println("\n***** Job 4/" + NUM_JOBS + " Starting *****\n");
		JobGroup jobGroupFour = new JobGroup("Job 4");

		/* Job 4a */
		// K Factor accuracy ranker
		// Input: Output from job 3c
		// Output: KFactor %Correct (ranked highest percent to lowest percent)
		Configuration confFourA = new Configuration();
		Job jobFourA = Job.getInstance(confFourA);
		jobFourA.setJarByClass(Main.class);
		jobFourA.setMapperClass(KFactorAccuracyRankMapper.class);
		jobFourA.setReducerClass(KFactorAccuracyRankReducer.class);
		jobFourA.setMapOutputKeyClass(Text.class);
		jobFourA.setMapOutputValueClass(KFactorAccuracyWritable.class);
		jobFourA.setOutputKeyClass(NullWritable.class);
		jobFourA.setOutputValueClass(KFactorAccuracyWritable.class);
		jobFourA.setInputFormatClass(TextInputFormat.class);
		jobFourA.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobFourA, new Path(jobThreeCOutputPath));
		FileOutputFormat.setOutputPath(jobFourA, new Path(jobFourAOutputPath));
		jobGroupFour.addJob(jobFourA, "KFactor Accuracy Ranked");
		/* End Job 4a */

		/* Job 4b */
		// K Factor errorsquare ranker
		// Input: Output from job 3d
		// Output: KFactor errorSquare (ranked highest percent to lowest
		// percent)
		Configuration confFourB = new Configuration();
		Job jobFourB = Job.getInstance(confFourB);
		jobFourB.setJarByClass(Main.class);
		jobFourB.setMapperClass(KFactorAccuracyRankMapper.class);
		jobFourB.setReducerClass(KFactorAccuracyRankReducer.class);
		jobFourB.setMapOutputKeyClass(Text.class);
		jobFourB.setMapOutputValueClass(KFactorAccuracyWritable.class);
		jobFourB.setOutputKeyClass(NullWritable.class);
		jobFourB.setOutputValueClass(KFactorAccuracyWritable.class);
		jobFourB.setInputFormatClass(TextInputFormat.class);
		jobFourB.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobFourB, new Path(jobThreeDOutputPath));
		FileOutputFormat.setOutputPath(jobFourB, new Path(jobFourBOutputPath));
		jobGroupFour.addJob(jobFourB, "KFactor Error Square Ranked");
		/* End Job 4b */

		/* Job 4c */
		// K Factor error ranker
		// Input: Output from job 3e
		// Output: KFactor error (ranked highest percent to lowest percent)
		Configuration confFourC = new Configuration();
		Job jobFourC = Job.getInstance(confFourC);
		jobFourC.setJarByClass(Main.class);
		jobFourC.setMapperClass(KFactorAccuracyRankMapper.class);
		jobFourC.setReducerClass(KFactorAccuracyRankReducer.class);
		jobFourC.setMapOutputKeyClass(Text.class);
		jobFourC.setMapOutputValueClass(KFactorAccuracyWritable.class);
		jobFourC.setOutputKeyClass(NullWritable.class);
		jobFourC.setOutputValueClass(KFactorAccuracyWritable.class);
		jobFourC.setInputFormatClass(TextInputFormat.class);
		jobFourC.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobFourC, new Path(jobThreeEOutputPath));
		FileOutputFormat.setOutputPath(jobFourC, new Path(jobFourCOutputPath));
		jobGroupFour.addJob(jobFourC, "KFactor Error Ranked");
		/* End Job 4c */

		/* Job 4d */
		// K Factor true error ranker
		// Input: Output from job 3f
		// Output: KFactor true error (ranked highest percent to lowest percent)
		Configuration confFourD = new Configuration();
		Job jobFourD = Job.getInstance(confFourD);
		jobFourD.setJarByClass(Main.class);
		jobFourD.setMapperClass(KFactorAccuracyRankMapper.class);
		jobFourD.setReducerClass(KFactorAccuracyRankReducer.class);
		jobFourD.setMapOutputKeyClass(Text.class);
		jobFourD.setMapOutputValueClass(KFactorAccuracyWritable.class);
		jobFourD.setOutputKeyClass(NullWritable.class);
		jobFourD.setOutputValueClass(KFactorAccuracyWritable.class);
		jobFourD.setInputFormatClass(TextInputFormat.class);
		jobFourD.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobFourD, new Path(jobThreeFOutputPath));
		FileOutputFormat.setOutputPath(jobFourD, new Path(jobFourDOutputPath));
		jobGroupFour.addJob(jobFourD, "KFactor True Error Ranked");
		/* End Job 4d */

		jobGroupFour.runAndWait();
		if (!jobGroupFour.isSuccessful()) {
			System.err.println("\nERROR: Job 4 FAILED\n");
			System.exit(4);
		}
		System.out.println("\n***** Job 4/" + NUM_JOBS + " Finished *****\n");
		/* End Job 4 */

		System.exit(0);

	}

}
