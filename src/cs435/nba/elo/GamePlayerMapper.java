package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GamePlayerMapper extends Mapper<LongWritable, Text, IntWritable, GameWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// This map gets 2 files
		// Both are comma separated
		String[] vals = value.toString().split(",");

		if (vals.length == 23) {
			// The first file is the games file, it has columns of
			// 0. gameId
			// 1. seasonYear
			// 2. actualYear
			// 3. month
			// 4. day
			// 5. awayTeamId
			// 6. awayPoints (same as 13)
			// 7. awayTotalMinPlayed
			// 8. awayTotalRebounds
			// 9. awayTotalAssists
			// 10. awayTotalSteals
			// 11. awayTotalBlocks
			// 12. awayTotalTurnovers
			// 13. awayTotalPoints (same as 6)
			// 14. homeTeamId
			// 15. homePoints (same as 22)
			// 16. homeTotalMinPlayed
			// 17. homeTotalRebounds
			// 18. homeTotalAssists
			// 19. homeTotalSteals
			// 20. homeTotalBlocks
			// 21. homeTotalTurnovers
			// 22. homeTotalPoints (same as 15)
			String gameId = vals[0];
			int year = Integer.parseInt(vals[2]);
			int month = Integer.parseInt(vals[3]);
			int day = Integer.parseInt(vals[4]);

			GameWritable game = new GameWritable(gameId, year, month, day);

			String awayTeamId = vals[5];
			int awayPoints = Integer.parseInt(vals[6]);
			int awayMinPlayed = Integer.parseInt(vals[7]);
			int awayRebounds = Integer.parseInt(vals[8]);
			int awayAssists = Integer.parseInt(vals[9]);
			int awaySteals = Integer.parseInt(vals[10]);
			int awayBlocks = Integer.parseInt(vals[11]);
			int awayTurnovers = Integer.parseInt(vals[12]);

			TeamGameWritable awayTeam = new TeamGameWritable(awayTeamId, awayPoints, awayMinPlayed, awayRebounds,
					awayAssists, awaySteals, awayBlocks, awayTurnovers);

			String homeTeamId = vals[14];
			int homePoints = Integer.parseInt(vals[15]);
			int homeMinPlayed = Integer.parseInt(vals[16]);
			int homeRebounds = Integer.parseInt(vals[17]);
			int homeAssists = Integer.parseInt(vals[18]);
			int homeSteals = Integer.parseInt(vals[19]);
			int homeBlocks = Integer.parseInt(vals[20]);
			int homeTurnovers = Integer.parseInt(vals[21]);

			TeamGameWritable homeTeam = new TeamGameWritable(homeTeamId, homePoints, homeMinPlayed, homeRebounds,
					homeAssists, homeSteals, homeBlocks, homeTurnovers);

			game.setAwayTeam(awayTeam);
			game.setHomeTeam(homeTeam);

		} else if (vals.length == 11) {
			// The second file is the player file, it has columns of
			// 0. gameId
			// 1. teamId
			// 2. playerId
			// 3. playerName
			// 4. minPlayed
			// 5. rebounds
			// 6. assists
			// 7. steals
			// 8. blocks
			// 9. turnovers
			// 10. points

		} else {
			System.err.println("File has vals length = " + vals.length + ". expecting 23 or 11");
		}
	}
}
