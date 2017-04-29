package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KFactorAccuracyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Text we get starts with k value, then tab then comma separated list
		// comma separated list is
		// 0. gameID
		// 1. year
		// 2. month
		// 3. day
		// 4. homeTeamId
		// 5. homeTeamPionts
		// 6. homeTeamStartElo
		// 7. homeTeamEndElo
		// 8. awayTeamId
		// 9. awayTeamPoints
		// 10. awayTeamStartElo
		// 11. awayTeamEndElo
		// 12. homeTeamId
		// 13+i. homePlayerId
		// 14+i. homePlayerStartElo
		// 15+i. homePlayerEndElo
		// n. awayTeamId
		// n+x. awayPlayerId
		// n+1+x. awayPlayerStartElo
		// n+2+x. awayPlayerEndElo

		String line = value.toString();
		String kFactor = line.split("\\s+")[0];
		String commaLine = line.split("\\s+")[1];
		String[] tokens = commaLine.split(",");
		double homeTeamStartElo = Double.parseDouble(tokens[6]);
		double awayTeamStartElo = Double.parseDouble(tokens[10]);
		double homeTeamPoints = Double.parseDouble(tokens[5]);
		double awayTeamPoints = Double.parseDouble(tokens[9]);

		// Who we predicted is the team with the higher startElo
		// System.out.println("k: " + kFactor + " home: " + homeTeamStartElo +
		// "," + homeTeamPoints + " away: "
		// + awayTeamStartElo + "," + awayTeamPoints);
		if (homeTeamStartElo > awayTeamStartElo) {

			// Predicted home win
			if (homeTeamPoints > awayTeamPoints) {

				// Correctly predicted
				context.write(new Text(kFactor), new IntWritable(1));

			} else {

				// Incorrect prediction
				context.write(new Text(kFactor), new IntWritable(0));
			}

		} else if (homeTeamStartElo < awayTeamStartElo) {

			// Predicted away win
			if (awayTeamPoints > homeTeamPoints) {

				// Correctly predicted
				context.write(new Text(kFactor), new IntWritable(1));

			} else {

				// Incorrect prediction
				context.write(new Text(kFactor), new IntWritable(0));
			}

		}
		// else we predicted they would tie, throw it out (should only happen
		// for first games)
	}
}
