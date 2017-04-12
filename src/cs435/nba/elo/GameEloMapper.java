package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GameEloMapper extends Mapper<LongWritable, Text, KFactorDateWritable, GameWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// This map gets a single file, comma separated
		// 0. gameId
		// 1. year
		// 2. month
		// 3. day
		// 4. awayTeamId
		// 5. awayPoints
		// 6. awayMinPlayed
		// 7. awayRebounds
		// 8. awayAssists
		// 9. awaySteals
		// 10. awayBlocks
		// 11. awayTurnovers
		// 12. homeTeamId
		// 13. homePoints
		// 14. homeMinPlayed
		// 15. homeRebounds
		// 16. homeAssists
		// 17. homeSteals
		// 18. homeBlocks
		// 19. homeTurnovers
		// 20 + (i*10). playerTeamId
		// 21 + (i*10). playerId
		// 22 + (i*10). playerName
		// 23 + (i*10). playerMinPlayed
		// 24 + (i*10). playerRebounds
		// 25 + (i*10). playerAssists
		// 26 + (i*10). playerSteals
		// 27 + (i*10). playerBlocks
		// 28 + (i*10). playerTurnovers
		// 29 + (i*10). playerPoints
		String[] vals = value.toString().split(",");

		String gameId = vals[0];
		int year = Integer.parseInt(vals[1]);
		int month = Integer.parseInt(vals[2]);
		int day = Integer.parseInt(vals[3]);

		GameWritable game = new GameWritable(gameId, year, month, day);

		String awayTeamId = vals[4];
		double awayPoints = Double.parseDouble(vals[5]);
		double awayMinPlayed = Double.parseDouble(vals[6]);
		double awayRebounds = Double.parseDouble(vals[7]);
		double awayAssists = Double.parseDouble(vals[8]);
		double awaySteals = Double.parseDouble(vals[9]);
		double awayBlocks = Double.parseDouble(vals[10]);
		double awayTurnovers = Double.parseDouble(vals[11]);

		TeamGameWritable awayTeam = new TeamGameWritable(awayTeamId, awayPoints, awayMinPlayed, awayRebounds,
				awayAssists, awaySteals, awayBlocks, awayTurnovers);
		game.setAwayTeam(awayTeam);

		String homeTeamId = vals[12];
		double homePoints = Double.parseDouble(vals[13]);
		double homeMinPlayed = Double.parseDouble(vals[14]);
		double homeRebounds = Double.parseDouble(vals[15]);
		double homeAssists = Double.parseDouble(vals[16]);
		double homeSteals = Double.parseDouble(vals[17]);
		double homeBlocks = Double.parseDouble(vals[18]);
		double homeTurnovers = Double.parseDouble(vals[19]);

		TeamGameWritable homeTeam = new TeamGameWritable(homeTeamId, homePoints, homeMinPlayed, homeRebounds,
				homeAssists, homeSteals, homeBlocks, homeTurnovers);
		game.setHomeTeam(homeTeam);

		int index = 20;
		int offset = 10;
		System.out.println("mapper vals.length: " + vals.length);
		while (index + offset <= vals.length) {

			String playerTeamId = vals[index];
			String playerId = vals[index + 1];
			String playerName = vals[index + 2];
			double playerMinPlayed = Double.parseDouble(vals[index + 3]);
			double playerRebounds = Double.parseDouble(vals[index + 4]);
			double playerAssists = Double.parseDouble(vals[index + 5]);
			double playerSteals = Double.parseDouble(vals[index + 6]);
			double playerBlocks = Double.parseDouble(vals[index + 7]);
			double playerTurnovers = Double.parseDouble(vals[index + 8]);
			double playerPoints = Double.parseDouble(vals[index + 9]);

			PlayerGameWritable player = new PlayerGameWritable(playerTeamId, playerId, playerName, playerMinPlayed,
					playerRebounds, playerAssists, playerSteals, playerBlocks, playerTurnovers, playerPoints);

			try {
				game.addPlayer(player);
				System.out.println("mapper added player: " + player.getPlayerId() + " to game: " + game.getGameId());
			} catch (TeamNotFoundException e) {
				System.out.println(
						"mapper could NOT add player: " + player.getPlayerId() + " to game: " + game.getGameId());
				e.printStackTrace();
			}

			index += offset;
		}

		// TODO change this to write to different K values
		try {
			System.out.println("mapper #homePlayers; " + game.getHomeTeam().getPlayers().size());
			System.out.println("mapper #awayPlayers: " + game.getAwayTeam().getPlayers().size());
		} catch (TeamNotFoundException e) {
			e.printStackTrace();
		}
		KFactorDateWritable kFactorKey = new KFactorDateWritable(Constants.TEST_K_FACTOR, game.getYear(),
				game.getMonth(), game.getDay());
		context.write(kFactorKey, game);
	}
}
