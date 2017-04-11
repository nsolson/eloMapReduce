package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a player in a single game. Implements {@link WritableComparable}
 * so it can be used in Hadoop's map and reduce.
 * 
 * TODO Need to override the toString method of this to make printing to files
 * easier
 * 
 * @author nate
 *
 */
public class PlayerGameWritable implements WritableComparable<PlayerGameWritable> {

	/**
	 * The ID of the team the player plays for
	 */
	private String teamId;

	/**
	 * The ID of the player
	 */
	private String playerId;

	/**
	 * The name of the player
	 */
	private String name;

	/**
	 * Total points scored by this player
	 */
	private double points;

	/**
	 * Total minutes played by this player
	 */
	private double minPlayed;

	/**
	 * Total rebounds by this player
	 */
	private double rebounds;

	/**
	 * Total assists by this player
	 */
	private double assists;

	/**
	 * Total steals by this player
	 */
	private double steals;

	/**
	 * Total blocks by this player
	 */
	private double blocks;

	/**
	 * Total turnovers by this player
	 */
	private double turnovers;

	/**
	 * Default constructor, required for Hadoop
	 */
	public PlayerGameWritable() {
		this(Constants.INVALID_ID, Constants.INVALID_ID, Constants.EMPTY_STRING, Constants.INVALID_STAT,
				Constants.INVALID_STAT, Constants.INVALID_STAT, Constants.INVALID_STAT, Constants.INVALID_STAT,
				Constants.INVALID_STAT, Constants.INVALID_STAT);
	}

	/**
	 * Constructor that initializes all values
	 * 
	 * @param teamId
	 *            The ID of the team this player plays for
	 * @param playerId
	 *            The ID of this player
	 * @param name
	 *            The name of the player
	 * @param points
	 *            The points scored by this player
	 * @param minPlayed
	 *            Total minutes played by this player
	 * @param rebounds
	 *            Total rebounds by this player
	 * @param assists
	 *            Total assists by this player
	 * @param steals
	 *            Total steals by this player
	 * @param blocks
	 *            Total blocks by this player
	 * @param turnovers
	 *            Total turnovers by this player
	 */
	public PlayerGameWritable(String teamId, String playerId, String name, double points, double minPlayed,
			double rebounds, double assists, double steals, double blocks, double turnovers) {

		this.teamId = teamId;
		this.playerId = playerId;
		this.name = name;
		this.points = points;
		this.minPlayed = minPlayed;
		this.rebounds = rebounds;
		this.assists = assists;
		this.steals = steals;
		this.blocks = blocks;
		this.turnovers = turnovers;
	}

	/**
	 * @return {@link PlayerGameWritable#teamId}
	 */
	public String getTeamId() {
		return teamId;
	}

	/**
	 * @return {@link PlayerGameWritable#playerId}
	 */
	public String getPlayerId() {
		return playerId;
	}

	/**
	 * @return {@link PlayerGameWritable#name}
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return {@link PlayerGameWritable#points}
	 */
	public double getPoints() {
		return points;
	}

	/**
	 * @return {@link PlayerGameWritable#minPlayed}
	 */
	public double getMinPlayed() {
		return minPlayed;
	}

	/**
	 * @return {@link PlayerGameWritable#rebounds}
	 */
	public double getRebounds() {
		return rebounds;
	}

	/**
	 * @return {@link PlayerGameWritable#assists}
	 */
	public double getAssists() {
		return assists;
	}

	/**
	 * @return {@link PlayerGameWritable#steals}
	 */
	public double getSteals() {
		return steals;
	}

	/**
	 * @return {@link PlayerGameWritable#blocks}
	 */
	public double getBlocks() {
		return blocks;
	}

	/**
	 * @return {@link PlayerGameWritable#turnovers}
	 */
	public double getTurnovers() {
		return turnovers;
	}

	/**
	 * Reads all the member variables from HDFS
	 * 
	 * @param in
	 *            The {@link DataInput}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		teamId = WritableUtils.readString(in);
		playerId = WritableUtils.readString(in);
		name = WritableUtils.readString(in);
		points = Double.parseDouble(WritableUtils.readString(in));
		minPlayed = Double.parseDouble(WritableUtils.readString(in));
		rebounds = Double.parseDouble(WritableUtils.readString(in));
		assists = Double.parseDouble(WritableUtils.readString(in));
		steals = Double.parseDouble(WritableUtils.readString(in));
		blocks = Double.parseDouble(WritableUtils.readString(in));
		turnovers = Double.parseDouble(WritableUtils.readString(in));
	}

	/**
	 * Writes all the member variables to HDFS
	 * 
	 * @param out
	 *            {@link DataOutput}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, teamId);
		WritableUtils.writeString(out, playerId);
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, Double.toString(points));
		WritableUtils.writeString(out, Double.toString(minPlayed));
		WritableUtils.writeString(out, Double.toString(rebounds));
		WritableUtils.writeString(out, Double.toString(assists));
		WritableUtils.writeString(out, Double.toString(steals));
		WritableUtils.writeString(out, Double.toString(blocks));
		WritableUtils.writeString(out, Double.toString(turnovers));
	}

	/**
	 * We don't really care how these are sorted (at least I don't think so). So
	 * I am just going to return the comparison of the teamID
	 * 
	 * @param other
	 *            The other {@link PlayerGameWritable} object to compare this
	 *            one to
	 * @return Negative, positive or 0 if this object is less than, greater than
	 *         or equal to the given object
	 */
	@Override
	public int compareTo(PlayerGameWritable other) {

		if (other == null) {
			return -1;
		}

		return playerId.compareTo(other.getPlayerId());
	}

	/**
	 * This function compares equality. The only thing we care about when
	 * comparing players is if their ID's are equal. If this object's ID is
	 * equal to {@link Constants#INVALID_ID} it should always return false
	 *
	 * @param o
	 *            The object to compare to
	 * @return true if:
	 *         <ul>
	 *         <li>The given object is an {@link PlayerGameWritable} with a
	 *         {@link PlayerGameWritable#playerId} equal to this one</li>
	 *         <li>The given object is an {@link PlayerEloWritable} with a
	 *         {@link PlayerEloWritable#getPlayerId} equal to this
	 *         {@link PlayerGameWritable#playerId}</li>
	 *         <li>If it is a {@link String} equal to this
	 *         {@link PlayerGameWritable#playerId}
	 *         <li>
	 *         </ul>
	 *         false otherwise
	 */
	@Override
	public boolean equals(Object o) {

		if (playerId.equals(Constants.INVALID_ID)) {
			return false;
		}

		if (o == null) {
			return false;
		}

		if (o instanceof PlayerGameWritable) {

			PlayerGameWritable other = (PlayerGameWritable) o;
			return playerId.equals(other.getPlayerId());

		} else if (o instanceof PlayerEloWritable) {

			PlayerEloWritable other = (PlayerEloWritable) o;
			return playerId.equals(other.getPlayerId());

		} else if (o instanceof String) {

			String otherPlayerId = (String) o;
			return playerId.equals(otherPlayerId);

		} else {

			// o not the same class
			return false;
		}

	}

	/**
	 * It is required in Hadoop to return consistent hashcodes across instances
	 * of JVMs. Luckily for us {@link String#hashCode} already does this. This
	 * is also so when adding to HashSets or HashMaps, we properly add or don't
	 * add.
	 * 
	 * @return The hashCode of {@link PlayerGameWritable#playerId}
	 */
	@Override
	public int hashCode() {
		return playerId.hashCode();
	}
}
