package cs435.nba.elo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GamePlayerReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// This reduce gets output from 2 files
		// Both are comma separated
		GameWritable game = null;
		TeamGameWritable awayTeam = null;
		TeamGameWritable homeTeam = null;
		Set<PlayerGameWritable> players = new HashSet<PlayerGameWritable>();

		for (Text value : values) {

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

				game = new GameWritable(gameId, year, month, day);

				String awayTeamId = vals[5];
				double awayPoints = Double.parseDouble(vals[6]);
				double awayMinPlayed = Double.parseDouble(vals[7]);
				double awayRebounds = Double.parseDouble(vals[8]);
				double awayAssists = Double.parseDouble(vals[9]);
				double awaySteals = Double.parseDouble(vals[10]);
				double awayBlocks = Double.parseDouble(vals[11]);
				double awayTurnovers = Double.parseDouble(vals[12]);

				awayTeam = new TeamGameWritable(awayTeamId, awayPoints, awayMinPlayed, awayRebounds, awayAssists,
						awaySteals, awayBlocks, awayTurnovers);

				String homeTeamId = vals[14];
				double homePoints = Double.parseDouble(vals[15]);
				double homeMinPlayed = Double.parseDouble(vals[16]);
				double homeRebounds = Double.parseDouble(vals[17]);
				double homeAssists = Double.parseDouble(vals[18]);
				double homeSteals = Double.parseDouble(vals[19]);
				double homeBlocks = Double.parseDouble(vals[20]);
				double homeTurnovers = Double.parseDouble(vals[21]);

				homeTeam = new TeamGameWritable(homeTeamId, homePoints, homeMinPlayed, homeRebounds, homeAssists,
						homeSteals, homeBlocks, homeTurnovers);

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
				String gameId = vals[0];
				String teamId = vals[1];
				String playerId = vals[2];
				String playerName = vals[3];
				double minPlayed = Double.parseDouble(vals[4]);
				double rebounds = Double.parseDouble(vals[5]);
				double assists = Double.parseDouble(vals[6]);
				double steals = Double.parseDouble(vals[7]);
				double blocks = Double.parseDouble(vals[8]);
				double turnovers = Double.parseDouble(vals[9]);
				double points = Double.parseDouble(vals[10]);

				// Just add this player to the home team?
				PlayerGameWritable player = new PlayerGameWritable(teamId, playerId, playerName, points, minPlayed,
						rebounds, assists, steals, blocks, turnovers);

				players.add(player);

			} else {

				System.err.println("File has vals length = " + vals.length + ". expecting 23 or 11");
			}
		}

		String str = "";
		if (game != null && homeTeam != null && awayTeam != null) {

			// Game info
			str += game.getGameId() + "," + game.getYear() + "," + game.getMonth() + "," + game.getDay();

			// awayTeam Info
			str += "," + awayTeam.getTeamId() + "," + awayTeam.getPoints() + "," + awayTeam.getMinPlayed() + ","
					+ awayTeam.getRebounds() + "," + awayTeam.getAssists() + "," + awayTeam.getSteals() + ","
					+ awayTeam.getBlocks() + "," + awayTeam.getTurnovers();

			// homeTeam Info
			str += "," + homeTeam.getTeamId() + "," + homeTeam.getPoints() + "," + homeTeam.getMinPlayed() + ","
					+ homeTeam.getRebounds() + "," + homeTeam.getAssists() + "," + homeTeam.getSteals() + ","
					+ homeTeam.getBlocks() + "," + homeTeam.getTurnovers();

			// Player info
			for (PlayerGameWritable player : players) {

				str += "," + player.getTeamId() + "," + player.getPlayerId() + "," + player.getName() + ","
						+ player.getMinPlayed() + "," + player.getRebounds() + "," + player.getAssists() + ","
						+ player.getSteals() + "," + player.getBlocks() + "," + player.getTurnovers() + ","
						+ player.getPoints();
			}

			context.write(NullWritable.get(), new Text(str));

		} else {
			System.out.println("Could not get game, homeTeam or awayTeam for gameId: " + key);
		}

	}
}
