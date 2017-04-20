package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
	 * The ID of the game
	 */
	private String gameId;

	/**
	 * The season year of the game (not actual year)
	 */
	private int seasonYear;

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
	 * The home team for this game
	 */
	private TeamGameWritable homeTeam;

	/**
	 * The away team for this game
	 */
	private TeamGameWritable awayTeam;

	/**
	 * Default constructor, necessary for Hadoop
	 */
	public GameWritable() {
		// Call actual constructor with known invalid parameters
		this(Constants.INVALID_ID, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE,
				Constants.INVALID_DATE);
	}

	/**
	 * For the player file case, we only know the gameID
	 * 
	 * @param gameId
	 *            The ID of the game
	 */
	public GameWritable(String gameId) {
		this(gameId, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE);
	}

	/**
	 * Initializes this object with necessary information
	 * 
	 * @param gameId
	 *            The ID of the game
	 * @param seasonYear
	 *            The season year of the game
	 * @param year
	 *            The actual year of the game (not the season year)
	 * @param month
	 *            The actual month of the game
	 * @param day
	 *            The actual day of the game
	 */
	public GameWritable(String gameId, int seasonYear, int year, int month, int day) {
		this.gameId = gameId;
		this.seasonYear = seasonYear;
		this.year = year;
		this.month = month;
		this.day = day;
		homeTeam = null;
		awayTeam = null;
	}

	/**
	 * Copy constructor
	 * 
	 * @param game
	 *            The {@link GameWritable} object to copy
	 */
	public GameWritable(GameWritable game) {

		this();

		if (game != null) {
			this.gameId = new String(game.getGameId());
			this.seasonYear = game.getSeasonYear();
			this.year = game.getYear();
			this.month = game.getMonth();

			try {
				this.setHomeTeam(new TeamGameWritable(game.getHomeTeam()));
			} catch (TeamNotFoundException e) {
				e.printStackTrace();
			}
			try {
				this.setAwayTeam(new TeamGameWritable(game.getAwayTeam()));
			} catch (TeamNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @return The {@link GameWritable#gameId}
	 */
	public String getGameId() {
		return gameId;
	}

	/**
	 * @return The {@link GameWritable#seasonYear}
	 */
	public int getSeasonYear() {
		return seasonYear;
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
	 * @return true if the {@link GameWritable#homeTeam} scored more points than
	 *         the {@link GameWritable#awayTeam}, false otherwise
	 */
	public boolean isHomeWinner() {

		if (homeTeam != null && awayTeam != null) {

			double homePoints = homeTeam.getPoints();
			double awayPoints = awayTeam.getPoints();

			return homePoints > awayPoints;
		}

		return false;
	}

	/**
	 * @return true if the {@link GameWritable#awayTeam} scored more points than
	 *         the {@link GameWritable#homeTeam}, false otherwise
	 */
	public boolean isAwayWinner() {

		if (homeTeam != null && awayTeam != null) {

			double homePoints = homeTeam.getPoints();
			double awayPoints = awayTeam.getPoints();

			return awayPoints > homePoints;
		}

		return false;
	}

	/**
	 * @return true if {@link GameWritable#homeTeam} is not null, false
	 *         otherwise
	 */
	public boolean hasHomeTeam() {

		return homeTeam != null;
	}

	/**
	 * @return true if {@link GameWritable#awayTeam} is not null, false
	 *         otherwise
	 */
	public boolean hasAwayTeam() {
		return awayTeam != null;
	}

	/**
	 * @return {@link GameWritable#homeTeam}
	 * @throws TeamNotFoundException
	 *             If {@link GameWritable#homeTeam} is null
	 */
	public TeamGameWritable getHomeTeam() throws TeamNotFoundException {

		if (homeTeam != null) {
			return homeTeam;
		} else {
			throw new TeamNotFoundException("Game: " + gameId + " does not have a home team");
		}
	}

	/**
	 * @return {@link GameWritable#awayTeam}
	 * @throws TeamNotFoundException
	 *             If {@link GameWritable#awayTeam} is null
	 */
	public TeamGameWritable getAwayTeam() throws TeamNotFoundException {

		if (awayTeam != null) {
			return awayTeam;
		} else {
			throw new TeamNotFoundException("Game: " + gameId + " does not have an away team");
		}
	}

	/**
	 * Sets {@link GameWritable#homeTeam}
	 * 
	 * @param homeTeam
	 *            The {@link TeamGameWritable} representing the home team for
	 *            this game.
	 */
	public void setHomeTeam(TeamGameWritable homeTeam) {

		this.homeTeam = homeTeam;
	}

	/**
	 * Sets {@link GameWritable#awayTeam}
	 * 
	 * @param awayTeam
	 *            The {@link TeamGameWritable} representing the away team for
	 *            this game.
	 */
	public void setAwayTeam(TeamGameWritable awayTeam) {

		this.awayTeam = awayTeam;
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
		if (awayTeam != null) {

			if (awayTeam.equals(teamId)) {

				awayTeam.addPlayer(player);
				playerAdded = true;
			}
		}

		if (homeTeam != null) {

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
	 * Reads fields from HDFS into this class
	 * 
	 * @param in
	 *            The {@link DataInput}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		gameId = WritableUtils.readString(in);
		seasonYear = Integer.parseInt(WritableUtils.readString(in));
		year = Integer.parseInt(WritableUtils.readString(in));
		month = Integer.parseInt(WritableUtils.readString(in));
		day = Integer.parseInt(WritableUtils.readString(in));

		if (homeTeam == null) {
			homeTeam = new TeamGameWritable();
		}
		homeTeam.readFields(in);

		if (awayTeam == null) {
			awayTeam = new TeamGameWritable();
		}
		awayTeam.readFields(in);
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
		WritableUtils.writeString(out, Integer.toString(seasonYear));
		WritableUtils.writeString(out, Integer.toString(year));
		WritableUtils.writeString(out, Integer.toString(month));
		WritableUtils.writeString(out, Integer.toString(day));

		if (homeTeam == null) {
			homeTeam = new TeamGameWritable();
		}
		homeTeam.write(out);

		if (awayTeam == null) {
			awayTeam = new TeamGameWritable();
		}
		awayTeam.write(out);
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

		if (seasonYear == other.getSeasonYear()) {

			if (year == other.getYear()) {

				if (month == other.getMonth()) {

					if (day == other.getDay()) {

						// Same date, return comparison of gameIDs
						return gameId.compareTo(other.getGameId());

					} else {

						// month and year are equal
						// day - otherDay will be negative if our day is
						// earlier,
						// positive if our day is later
						return day - other.getDay();
					}

				} else {

					// year is equal
					// month - otherMonth will be negative if our month is
					// earlier,
					// positive if our month is later
					return month - other.getMonth();
				}

			} else {

				// year - otherYear will be negative if our year is earlier,
				// positive if our year is later
				return year - other.getYear();
			}

		} else {

			// seasonYear - otherSeasonYear will be negative if our year is
			// earlier, positive if our year is later
			return seasonYear - other.getSeasonYear();
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
