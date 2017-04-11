package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a single game of an NBA season. Implements
 * {@link WritableComparable} so it can be used in Hadoop's map and reduce
 * 
 * TODO Need to override the toString method of this to make printing to files
 * easier
 * 
 * @author nate
 *
 */
public class GameWritable implements WritableComparable<GameWritable> {

	/**
	 * ID representing the home team
	 */
	private static final Text HOME_TEAM = new Text("HOME");

	/**
	 * ID representing the away team
	 */
	private static final Text AWAY_TEAM = new Text("AWAY");

	/**
	 * The ID of the game
	 */
	private String gameId;

	/**
	 * The actual year of the game (not season year)
	 */
	private int year;

	/**
	 * The actual month of the game
	 */
	private int month;

	/**
	 * The actual day of the game
	 */
	private int day;

	/**
	 * A {@link MapWritable} to keep our teams
	 */
	private MapWritable teams;

	/**
	 * Default constructor, necessary for Hadoop
	 */
	public GameWritable() {
		// Call actual constructor with known invalid parameters
		this(Constants.INVALID_ID, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE);
	}

	/**
	 * For the player file case, we only know the gameID
	 * 
	 * @param gameId
	 *            The ID of the game
	 */
	public GameWritable(String gameId) {
		this(gameId, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE);
	}

	/**
	 * Initializes this object with necessary information
	 * 
	 * @param gameId
	 *            The ID of the game
	 * @param year
	 *            The actual year of the game (not the season year)
	 * @param month
	 *            The actual month of the game
	 * @param day
	 *            The actual day of the game
	 */
	public GameWritable(String gameId, int year, int month, int day) {
		this.gameId = gameId;
		this.year = year;
		this.month = month;
		this.day = day;
		teams = new MapWritable();
	}

	/**
	 * @return The {@link GameWritable#gameId}
	 */
	public String getGameId() {
		return gameId;
	}

	/**
	 * @return The {@link GameWritable#year}
	 */
	public int getYear() {
		return year;
	}

	/**
	 * @return The {@link GameWritable#month}
	 */
	public int getMonth() {
		return month;
	}

	/**
	 * @return The {@link GameWritable#day}
	 */
	public int getDay() {
		return day;
	}

	/**
	 * @return true if there is a home team in {@link GameWritable#teams}, false
	 *         otherwise
	 */
	public boolean hasHomeTeam() {

		return teams.containsKey(HOME_TEAM);
	}

	/**
	 * @return true if there is an away team in {@link GameWritable#teams},
	 *         false otherwise
	 */
	public boolean hasAwayTeam() {
		return teams.containsKey(AWAY_TEAM);
	}

	/**
	 * @return The home team from {@link GameWritable#teams}
	 * @throws TeamNotFoundException
	 *             if there is no home team set for this game
	 */
	public TeamGameWritable getHomeTeam() throws TeamNotFoundException {

		if (teams.containsKey(HOME_TEAM)) {
			return (TeamGameWritable) teams.get(HOME_TEAM);
		} else {
			throw new TeamNotFoundException("Game: " + gameId + " does not have a home team");
		}
	}

	/**
	 * @return The away team from {@link GameWritable#teams}
	 * @throws TeamNotFoundException
	 *             if there is no away team set for this game
	 */
	public TeamGameWritable getAwayTeam() throws TeamNotFoundException {

		if (teams.containsKey(AWAY_TEAM)) {
			return (TeamGameWritable) teams.get(AWAY_TEAM);
		} else {
			throw new TeamNotFoundException("Game: " + gameId + " does not have an away team");
		}
	}

	/**
	 * Sets the home team for {@link GameWritable#teams}
	 * 
	 * @param homeTeam
	 *            The {@link TeamGameWritable} representing the home team for
	 *            this game.
	 */
	public void setHomeTeam(TeamGameWritable homeTeam) {

		if (teams.containsKey(HOME_TEAM)) {
			teams.remove(HOME_TEAM);
		}

		teams.put(HOME_TEAM, homeTeam);
	}

	/**
	 * Sets the away team for {@link GameWritable#teams}
	 * 
	 * @param awayTeam
	 *            The {@link TeamGameWritable} representing the away team for
	 *            this game.
	 */
	public void setAwayTeam(TeamGameWritable awayTeam) {

		if (teams.containsKey(AWAY_TEAM)) {
			teams.remove(AWAY_TEAM);
		}

		teams.put(AWAY_TEAM, awayTeam);
	}

	/**
	 * Adds the given player to the correct team
	 * 
	 * @param player
	 *            The {@link PlayerGameWritable} to add
	 * @throws TeamNotFoundException
	 *             If the player team is not a part of this game
	 */
	public void addPlayer(PlayerGameWritable player) throws TeamNotFoundException {

		if (player == null) {
			return;
		}

		boolean playerAdded = false;
		String teamId = player.getTeamId();
		if (teams.containsKey(AWAY_TEAM)) {

			TeamGameWritable awayTeam = (TeamGameWritable) teams.get(AWAY_TEAM);
			if (awayTeam.equals(teamId)) {

				awayTeam.addPlayer(player);
				playerAdded = true;
			}
		}

		if (teams.containsKey(HOME_TEAM)) {

			TeamGameWritable homeTeam = (TeamGameWritable) teams.get(HOME_TEAM);
			if (homeTeam.equals(teamId)) {

				homeTeam.addPlayer(player);
				playerAdded = true;
			}
		}

		if (!playerAdded) {
			throw new TeamNotFoundException("Could not find teamId: " + teamId + "in game: " + gameId);
		}
	}

	/**
	 * 
	 */

	/**
	 * Reads fields from HDFS into this class
	 * 
	 * @param in
	 *            The {@link DataInput}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		gameId = WritableUtils.readString(in);
		year = Integer.parseInt(WritableUtils.readString(in));
		month = Integer.parseInt(WritableUtils.readString(in));
		day = Integer.parseInt(WritableUtils.readString(in));
		teams.readFields(in);
	}

	/**
	 * Writes fields from this class to HDFS
	 * 
	 * @param out
	 *            The {@link DataOutput}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, gameId);
		WritableUtils.writeString(out, Integer.toString(year));
		WritableUtils.writeString(out, Integer.toString(month));
		WritableUtils.writeString(out, Integer.toString(day));
		teams.write(out);
	}

	/**
	 * Compares this object to the given object. We want to sort earliest game
	 * to latest game. If they have the same date, we will simply sort on
	 * gameID.
	 * 
	 * @param other
	 *            The {@link GameWritable} to compare this one to
	 * @return Negative, positive or 0 if this objects date is earlier, later or
	 *         the same respectively. Even if the dates are the same, we will
	 *         return this objects {@link GameWritable#gameId} to the other
	 *         objects {@link GameWritable#gameId}
	 */
	@Override
	public int compareTo(GameWritable other) {

		if (other == null) {
			return -1;
		}

		if (year == other.getYear()) {

			if (month == other.getMonth()) {

				if (day == other.getDay()) {

					// Same date, return comparison of gameIDs
					return gameId.compareTo(other.getGameId());

				} else {

					// month and year are equal
					// day - otherDay will be negative if our day is earlier,
					// positive if our day is later
					return day - other.getDay();
				}

			} else {

				// year is equal
				// month - otherMonth will be negative if our month is earlier,
				// positive if our month is later
				return month - other.getMonth();
			}

		} else {

			// year - otherYear will be negative if our year is earlier,
			// positive if our year is later
			return year - other.getYear();
		}
	}

	/**
	 * This function compares equality. The only thing we care about when
	 * comparing games is if their ID's are equal. If this object's ID is equal
	 * to {@link Constants#INVALID_ID} it should always return false
	 * 
	 * @param o
	 *            The object to compare this one to
	 * @return true if:
	 *         <ul>
	 *         <li>The other object is an instance of {@link GameWritable} and
	 *         has a {@link GameWritable#gameId} equal to this one</li>
	 *         <li>The other object is a {@link String} that is equal to this
	 *         {@link GameWritable#gameId}</li>
	 *         </ul>
	 *         false otherwise
	 */
	@Override
	public boolean equals(Object o) {

		// If we have an invalid id it shouldn't be equal to anything
		if (gameId.equals(Constants.INVALID_ID)) {
			return false;
		}

		if (o == null) {
			return false;
		}

		if (o instanceof GameWritable) {

			GameWritable other = (GameWritable) o;
			return gameId.equals(other.getGameId());

		} else if (o instanceof String) {

			String otherGameId = (String) o;
			return gameId.equals(otherGameId);

		} else {

			// o is not one of the classes we are looking for
			return false;
		}

	}

	/**
	 * It is required in Hadoop to return consistent hashcodes across instances
	 * of JVMs. Luckily for us {@link String#hashCode} already does this. This
	 * is also so when adding to HashSets or HashMaps, we properly add or don't
	 * add.
	 * 
	 * @return The hashCode of {@link GameWritable#gameId}
	 */
	@Override
	public int hashCode() {
		return gameId.hashCode();
	}

}
