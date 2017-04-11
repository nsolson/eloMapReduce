package cs435.nba.elo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class GameEloReducer extends Reducer<IntWritable, GameWritable, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<GameWritable> values, Context context)
			throws IOException, InterruptedException {

		// This should be able to calculate Elo values?
		// Need to iterate over entire set, extract games and extract player elo
		// values

		// The Key is our K-Factor
		double kFactor = (double) key.get();

		// Map<playerID, playerElo> for retreiving their Elo during calculations
		Map<String, PlayerEloWritable> playerEloMap = new HashMap<String, PlayerEloWritable>();

		// Use an arrayList to keep track of games because we can sort this
		// before calculating Elo
		List<GameWritable> sortedGames = new ArrayList<GameWritable>();
		for (GameWritable game : values) {

			// Add the game to the list of games
			// Use copy constructor instead of object reference as it will
			// disappear once iterated over
			sortedGames.add(new GameWritable(game));

			// Get the players from the game and add the to the playerEloMap, if
			// they don't already exist
			try {

				TeamGameWritable homeTeam = game.getHomeTeam();
				MapWritable players = homeTeam.getPlayers();
				Set<Writable> playerKeys = players.keySet();
				for (Writable playerKey : playerKeys) {

					PlayerGameWritable player = (PlayerGameWritable) players.get(playerKey);
					String playerId = new String(player.getPlayerId());

					// Only add it if not there
					if (!playerEloMap.containsKey(playerId)) {

						String playerName = new String(player.getName());
						playerEloMap.put(playerId, new PlayerEloWritable(playerId, playerName));
					}
				}
			} catch (TeamNotFoundException e) {

				System.err.println("Error game: " + game.getGameId() + " does not have a home team");
				e.printStackTrace();
			}

			try {

				TeamGameWritable awayTeam = game.getAwayTeam();
				MapWritable players = awayTeam.getPlayers();
				Set<Writable> playerKeys = players.keySet();
				for (Writable playerKey : playerKeys) {

					PlayerGameWritable player = (PlayerGameWritable) players.get(playerKey);
					String playerId = new String(player.getPlayerId());

					// Only add it if not there
					if (!playerEloMap.containsKey(playerId)) {

						String playerName = new String(player.getName());
						playerEloMap.put(playerId, new PlayerEloWritable(playerId, playerName));
					}
				}

			} catch (TeamNotFoundException e) {

				System.err.println("Error game: " + game.getGameId() + " does not have an away team");
				e.printStackTrace();
			}
		}

		// Sort the games so they are in order from earliest to latest
		Collections.sort(sortedGames);
		int gameId = 0;
		for (GameWritable game : sortedGames) {

			// For each game we need to:
			// 1. Set the starting elo for all the players
			// 2. Get the starting elo for both teams
			// 3. Figure out who won
			// 4. Calculate the change in Elo
			// 5. Update the change in Elo for the teams
			// 6. Update the elo value for the players in the playerEloMap so we
			// get correct values for the next game

			// 1. Set the starting elo for all the players
			game.setPlayersStartingElo(playerEloMap);

			try {

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

				// Start with numbers that come from tie
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
				// so we get correct values for the next game
				MapWritable homePlayers = game.getHomeTeam().getPlayers();
				Set<Writable> homeIds = homePlayers.keySet();
				for (Writable homeId : homeIds) {

					String homePlayerId = ((Text) homeId).toString();
					if (playerEloMap.containsKey(homePlayerId)) {
						double endElo = ((PlayerGameWritable) homePlayers.get(homeId)).getEndElo();
						playerEloMap.get(homePlayerId).setElo(endElo);
					}

				}

				MapWritable awayPlayers = game.getAwayTeam().getPlayers();
				Set<Writable> awayIds = awayPlayers.keySet();
				for (Writable awayId : awayIds) {

					String awayPlayerId = ((Text) awayId).toString();
					if (playerEloMap.containsKey(awayPlayerId)) {
						double endElo = ((PlayerGameWritable) awayPlayers.get(awayId)).getEndElo();
						playerEloMap.get(awayPlayerId).setElo(endElo);
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
				context.write(new IntWritable(++gameId), new Text(str));

			} catch (TeamNotFoundException e) {
				System.err.println("Error could not find both teams for gameId: " + game.getGameId());
				e.printStackTrace();
			}
		}
	}
}
