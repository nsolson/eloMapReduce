package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KFactorTrueErrorMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

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
		double rHome = Math.pow(10, homeTeamStartElo / 400);
		double rAway = Math.pow(10, awayTeamStartElo / 400);

		double eHome = rHome / (rHome + rAway);
		double eAway = rAway / (rHome + rAway);

		if (homeTeamStartElo > awayTeamStartElo) {

			// Predicted home win
			if (homeTeamPoints > awayTeamPoints) {

				// Correctly predicted
				// Our error is 0 (we got it right)
				context.write(new Text(kFactor), new DoubleWritable(0));

			} else {

				// Incorrect prediction
				// This means we should have predicted the other team higher (or
				// at minimum 50/50)
				// The error is what it takes to get them to 50/50 or the
				// absolute difference between the two
				// (e.g., if predicted 0.4 & 0.6, the abs dif is 0.2 which is
				// what it takes to get them both to 0.5)
				context.write(new Text(kFactor), new DoubleWritable(Math.abs(eHome - eAway)));
			}

		} else if (homeTeamStartElo < awayTeamStartElo) {

			// Predicted away win
			if (awayTeamPoints > homeTeamPoints) {

				// Correctly predicted
				// Our error is 0 (we got it right)
				context.write(new Text(kFactor), new DoubleWritable(0));

			} else {

				// Incorrect prediction
				// This means we should have predicted the other team higher (or
				// at minimum 50/50)
				// The error is what it takes to get them to 50/50 or the
				// absolute difference between the two
				// (e.g., if predicted 0.4 & 0.6, the abs dif is 0.2 which is
				// what it takes to get them both to 0.5)
				context.write(new Text(kFactor), new DoubleWritable(Math.abs(eAway - eHome)));
			}

		}
		// else we predicted they would tie, throw it out (should only happen
		// for first games)
	}
}
