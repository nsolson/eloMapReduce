package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KFactorBestTeamMapper extends Mapper<LongWritable, Text, DoubleWritable, IdEloWritable> {

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
		double kFactor = Double.parseDouble(line.split("\\s+")[0]);
		String commaLine = line.split("\\s+")[1];
		String[] tokens = commaLine.split(",");
		int year = Integer.parseInt(tokens[1]);
		int month = Integer.parseInt(tokens[2]);
		int day = Integer.parseInt(tokens[3]);
		String homeTeamId = tokens[4];
		String awayTeamId = tokens[8];
		double homeTeamElo = Double.parseDouble(tokens[7]);
		double awayTeamElo = Double.parseDouble(tokens[11]);

		IdEloWritable homeElo = new IdEloWritable(homeTeamId, year, month, day, homeTeamElo);
		IdEloWritable awayElo = new IdEloWritable(awayTeamId, year, month, day, awayTeamElo);

		context.write(new DoubleWritable(kFactor), homeElo);
		context.write(new DoubleWritable(kFactor), awayElo);
	}
}
