package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GameEloMapper extends Mapper<LongWritable, Text, KFactorDateWritable, GameWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// This map gets a single file, comma separated
		int gameIdIndex = 0;
		int seasonYearIndex = 1;
		int yearIndex = 2;
		int monthIndex = 3;
		int dayIndex = 4;
		int awayTeamIdIndex = 5;
		int awayPointsIndex = 6;
		int awayMinPlayedIndex = 7;
		int awayReboundsIndex = 8;
		int awayAssistsIndex = 9;
		int awayStealsIndex = 10;
		int awayBlocksIndex = 11;
		int awayTurnoversIndex = 12;
		int homeTeamIdIndex = 13;
		int homePointsIndex = 14;
		int homeMinPlayedIndex = 15;
		int homeReboundsIndex = 16;
		int homeAssistsIndex = 17;
		int homeStealsIndex = 18;
		int homeBlocksIndex = 19;
		int homeTurnoversIndex = 20;

		int playerTeamIdIndex = 21;
		int playerIdIndex = 22;
		int playerNameIndex = 23;
		int playerMinPlayedIndex = 24;
		int playerReboundsIndex = 25;
		int playerAssistsIndex = 26;
		int playerStealsIndex = 27;
		int playerBlocksIndex = 28;
		int playerTurnoversIndex = 29;
		int playerPointsIndex = 30;

		int numPlayerCols = 10;
		int playerIndexStart = 21;

		String[] vals = value.toString().split(",");

		String gameId = vals[gameIdIndex];
		int seasonYear = Integer.parseInt(vals[seasonYearIndex]);
		int year = Integer.parseInt(vals[yearIndex]);
		int month = Integer.parseInt(vals[monthIndex]);
		int day = Integer.parseInt(vals[dayIndex]);

		GameWritable game = new GameWritable(gameId, seasonYear, year, month, day);

		String awayTeamId = vals[awayTeamIdIndex];
		double awayPoints = Double.parseDouble(vals[awayPointsIndex]);
		double awayMinPlayed = Double.parseDouble(vals[awayMinPlayedIndex]);
		double awayRebounds = Double.parseDouble(vals[awayReboundsIndex]);
		double awayAssists = Double.parseDouble(vals[awayAssistsIndex]);
		double awaySteals = Double.parseDouble(vals[awayStealsIndex]);
		double awayBlocks = Double.parseDouble(vals[awayBlocksIndex]);
		double awayTurnovers = Double.parseDouble(vals[awayTurnoversIndex]);

		TeamGameWritable awayTeam = new TeamGameWritable(seasonYear, awayTeamId, awayPoints, awayMinPlayed,
				awayRebounds, awayAssists, awaySteals, awayBlocks, awayTurnovers);
		game.setAwayTeam(awayTeam);

		String homeTeamId = vals[homeTeamIdIndex];
		double homePoints = Double.parseDouble(vals[homePointsIndex]);
		double homeMinPlayed = Double.parseDouble(vals[homeMinPlayedIndex]);
		double homeRebounds = Double.parseDouble(vals[homeReboundsIndex]);
		double homeAssists = Double.parseDouble(vals[homeAssistsIndex]);
		double homeSteals = Double.parseDouble(vals[homeStealsIndex]);
		double homeBlocks = Double.parseDouble(vals[homeBlocksIndex]);
		double homeTurnovers = Double.parseDouble(vals[homeTurnoversIndex]);

		TeamGameWritable homeTeam = new TeamGameWritable(seasonYear, homeTeamId, homePoints, homeMinPlayed,
				homeRebounds, homeAssists, homeSteals, homeBlocks, homeTurnovers);
		game.setHomeTeam(homeTeam);

		// System.out.println("mapper vals.length: " + vals.length);
		int iteration = 0;

		while (playerIndexStart + (numPlayerCols * iteration) < vals.length) {

			String playerTeamId = vals[playerTeamIdIndex + (numPlayerCols * iteration)];
			String playerId = vals[playerIdIndex + (numPlayerCols * iteration)];
			String playerName = vals[playerNameIndex + (numPlayerCols * iteration)];
			double playerMinPlayed = Double.parseDouble(vals[playerMinPlayedIndex + (numPlayerCols * iteration)]);
			double playerRebounds = Double.parseDouble(vals[playerReboundsIndex + (numPlayerCols * iteration)]);
			double playerAssists = Double.parseDouble(vals[playerAssistsIndex + (numPlayerCols * iteration)]);
			double playerSteals = Double.parseDouble(vals[playerStealsIndex + (numPlayerCols * iteration)]);
			double playerBlocks = Double.parseDouble(vals[playerBlocksIndex + (numPlayerCols * iteration)]);
			double playerTurnovers = Double.parseDouble(vals[playerTurnoversIndex + (numPlayerCols * iteration)]);
			double playerPoints = Double.parseDouble(vals[playerPointsIndex + (numPlayerCols * iteration)]);

			PlayerGameWritable player = new PlayerGameWritable(playerTeamId, playerId, playerName, playerMinPlayed,
					playerRebounds, playerAssists, playerSteals, playerBlocks, playerTurnovers, playerPoints);

			try {
				game.addPlayer(player);
				// System.out.println("mapper added player: " +
				// player.getPlayerId() + " to game: " + game.getGameId());
			} catch (TeamNotFoundException e) {
				System.err.println(
						"mapper could NOT add player: " + player.getPlayerId() + " to game: " + game.getGameId());
				e.printStackTrace();
			}

			++iteration;
		}

		try {
			System.out.println("mapper #homePlayers; " + game.getHomeTeam().getPlayers().size());
			System.out.println("mapper #awayPlayers: " + game.getAwayTeam().getPlayers().size());
		} catch (TeamNotFoundException e) {
			e.printStackTrace();
		}

		if (Constants.TEST_RUN) {
			// For testing
			KFactorDateWritable kFactorKey = new KFactorDateWritable(Constants.TEST_K_FACTOR, game.getSeasonYear(),
					game.getYear(), game.getMonth(), game.getDay());
			context.write(kFactorKey, game);

		} else if (Constants.FINAL_RUN) {

			KFactorDateWritable kFactorKey = new KFactorDateWritable(Constants.FINAL_K_FACTOR, game.getSeasonYear(),
					game.getYear(), game.getMonth(), game.getDay());
			context.write(kFactorKey, game);

		} else {

			for (int kFactor = Constants.MIN_K_FACTOR; kFactor <= Constants.MAX_K_FACTOR; kFactor += Constants.K_FACTOR_STEP) {

				KFactorDateWritable kFactorKey = new KFactorDateWritable(kFactor, game.getSeasonYear(), game.getYear(),
						game.getMonth(), game.getDay());
				context.write(kFactorKey, game);
			}
		}

	}
}
