package cs435.nba.elo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class GameEloReducer extends Reducer<KFactorDateWritable, GameWritable, DoubleWritable, Text> {

	@Override
	public void reduce(KFactorDateWritable key, Iterable<GameWritable> values, Context context)
			throws IOException, InterruptedException {

		// Games are sorted earliest to latest so we can calc Elo one by one
		int gameNum = 0;

		double kFactor = key.getKFactor();

		Map<String, PlayerEloWritable> playerEloMap = new HashMap<String, PlayerEloWritable>();
		for (GameWritable game : values) {

			++gameNum;

			// 1. Set the starting elo for all the players
			// 2. Get the starting elo for both teams
			// 3. Figure out who won
			// 4. Calculate the change in Elo
			// 5. Update the change in Elo for the teams
			// 6. Update the elo value for the players in the playerEloMap so we
			// get correct values for the next game

			try {

				// 1. Set the starting elo for all the players
				// System.out.println("k: " + kFactor + " reducer game " +
				// gameNum + " #homePlayers; "
				// + game.getHomeTeam().getPlayers().size());
				// System.out.println("k: " + kFactor + " reducer game " +
				// gameNum + " #awayPlayers: "
				// + game.getAwayTeam().getPlayers().size());
				for (Writable id : game.getHomeTeam().getPlayers().keySet()) {

					String playerId = ((Text) id).toString();
					String playerName = "";
					try {

						PlayerGameWritable player = game.getHomeTeam().getPlayer(playerId);
						playerName = player.getName();

					} catch (PlayerNotFoundException e) {
						System.err.println("Could not find player: " + playerId + " even though they were in keyset");
						e.printStackTrace();
					}

					if (!playerEloMap.containsKey(playerId)) {

						playerEloMap.put(playerId, new PlayerEloWritable(playerId, playerName));
					}

					try {

						game.getHomeTeam().getPlayer(playerId).setStartElo(playerEloMap.get(playerId).getElo());

					} catch (PlayerNotFoundException e) {
						System.err.println("Could not find player: " + playerId + " even though they were in keyset");
						e.printStackTrace();
					}
				}

				for (Writable id : game.getAwayTeam().getPlayers().keySet()) {

					String playerId = ((Text) id).toString();
					String playerName = "";
					try {

						PlayerGameWritable player = game.getAwayTeam().getPlayer(playerId);
						playerName = player.getName();

					} catch (PlayerNotFoundException e) {
						System.err.println("Could not find player: " + playerId + " even though they were in keyset");
						e.printStackTrace();
					}

					if (!playerEloMap.containsKey(playerId)) {

						playerEloMap.put(playerId, new PlayerEloWritable(playerId, playerName));
					}

					try {

						game.getAwayTeam().getPlayer(playerId).setStartElo(playerEloMap.get(playerId).getElo());

					} catch (PlayerNotFoundException e) {
						System.err.println("Could not find player: " + playerId + " even though they were in keyset");
						e.printStackTrace();
					}
				}

				// 2. Get the starting elo for both teams
				double homeElo = game.getHomeTeam().getStartElo();
				double awayElo = game.getAwayTeam().getStartElo();

				// 3. Figure out who won
				boolean homeWin = game.isHomeWinner();
				boolean awayWin = game.isAwayWinner();

				// 4. Calculate the change in Elo
				double rHome = Math.pow(10, homeElo / 400);
				double rAway = Math.pow(10, awayElo / 400);

				double eHome = rHome / (rHome + rAway);
				double eAway = rAway / (rHome + rAway);

				// Start with numbers that represent tie
				double sHome = 0.5;
				double sAway = 0.5;
				if (homeWin) {

					sHome = 1;
					sAway = 0;

				} else if (awayWin) {

					sHome = 0;
					sAway = 1;
				}

				double homeEloChange = kFactor * (sHome - eHome);
				double awayEloChange = kFactor * (sAway - eAway);

				// 5. Update the change in Elo for the teams
				game.getHomeTeam().changeElo(homeEloChange);
				game.getAwayTeam().changeElo(awayEloChange);

				// 6. Update the elo value for the players in the playerEloMap
				// so we
				MapWritable homePlayers = game.getHomeTeam().getPlayers();
				for (Writable homeId : homePlayers.keySet()) {

					PlayerGameWritable homePlayer = (PlayerGameWritable) homePlayers.get(homeId);
					double endElo = homePlayer.getEndElo();

					String homePlayerId = ((Text) homeId).toString();
					if (playerEloMap.containsKey(homePlayerId)) {
						playerEloMap.get(homePlayerId).setElo(endElo);
					} else {
						playerEloMap.put(homePlayerId,
								new PlayerEloWritable(homePlayer.getPlayerId(), homePlayer.getName(), endElo));
					}
				}

				MapWritable awayPlayers = game.getAwayTeam().getPlayers();
				for (Writable awayId : awayPlayers.keySet()) {

					PlayerGameWritable awayPlayer = (PlayerGameWritable) awayPlayers.get(awayId);
					double endElo = awayPlayer.getEndElo();

					String awayPlayerId = ((Text) awayId).toString();
					if (playerEloMap.containsKey(awayPlayerId)) {
						playerEloMap.get(awayPlayerId).setElo(endElo);
					} else {
						playerEloMap.put(awayPlayerId,
								new PlayerEloWritable(awayPlayer.getPlayerId(), awayPlayer.getName(), endElo));
					}
				}

				// Write this game to our output
				String str = "";

				// Game
				str += game.getGameId() + "," + game.getYear() + "," + game.getMonth() + "," + game.getDay();

				// Home Team
				TeamGameWritable homeTeam = game.getHomeTeam();
				str += "," + homeTeam.getTeamId() + "," + homeTeam.getPoints() + "," + homeTeam.getStartElo() + ","
						+ homeTeam.getEndElo();

				// Away Team
				TeamGameWritable awayTeam = game.getAwayTeam();
				str += "," + awayTeam.getTeamId() + "," + awayTeam.getPoints() + "," + awayTeam.getStartElo() + ","
						+ awayTeam.getEndElo();

				// Home Players
				str += "," + homeTeam.getTeamId();
				for (Writable homeId : homePlayers.keySet()) {

					PlayerGameWritable player = (PlayerGameWritable) homePlayers.get(homeId);
					str += "," + player.getPlayerId() + "," + player.getStartElo() + "," + player.getEndElo();
				}

				// Away Players
				str += "," + awayTeam.getTeamId();
				for (Writable awayId : awayPlayers.keySet()) {

					PlayerGameWritable player = (PlayerGameWritable) awayPlayers.get(awayId);
					str += "," + player.getPlayerId() + "," + player.getStartElo() + "," + player.getEndElo();
				}

				// Write Game Elo Info out
				context.write(new DoubleWritable(kFactor), new Text(str));

			} catch (TeamNotFoundException e) {
				System.err.println("Could not get home and away team for gameId: " + game.getGameId());
				e.printStackTrace();
			}

		}
	}
}
