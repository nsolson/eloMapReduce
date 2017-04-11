package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This is to keep track of a single player for all games, it will simply hold
 * their ID and Elo value. The Elo value will be updated over time. Implements
 * {@link WritableComparable} so we can use it in Hadoop's map and reduce.
 * 
 * TODO Need to override the toString method of this to make printing to files
 * easier
 * 
 * @author nate
 *
 */
public class PlayerEloWritable implements WritableComparable<PlayerEloWritable> {

	/**
	 * The ID of this player
	 */
	private String playerId;

	/**
	 * The elo value of this player
	 */
	private int elo;

	/**
	 * Default constructor, required by Hadoop
	 */
	public PlayerEloWritable() {
		this(Constants.INVALID_ID);
	}

	/**
	 * Constructs this class with the ID and defaults their Elo to
	 * {@link Constants#START_ELO}
	 * 
	 * @param playerId
	 *            The ID of the player
	 */
	public PlayerEloWritable(String playerId) {
		this(playerId, Constants.START_ELO);
	}

	/**
	 * Constructs this class with the ID and the given Elo value
	 * 
	 * @param playerId
	 *            The ID of the player
	 * @param elo
	 *            The Elo value of the player
	 */
	public PlayerEloWritable(String playerId, int elo) {

		this.playerId = playerId;
		this.elo = elo;
	}

	/**
	 * @return {@link PlayerEloWritable#playerId}
	 */
	public String getPlayerId() {
		return playerId;
	}

	/**
	 * @return {@link PlayerEloWritable#elo}
	 */
	public int getElo() {
		return elo;
	}

	/**
	 * Reads the member variables from HDFS
	 * 
	 * @param in
	 *            The {@link DataInput}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		playerId = WritableUtils.readString(in);
		elo = Integer.parseInt(WritableUtils.readString(in));
	}

	/**
	 * Writes the member variables to HDFS
	 * 
	 * @param out
	 *            The {@link DataOutput}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, playerId);
		WritableUtils.writeString(out, Integer.toString(elo));
	}

	/**
	 * I don't think we care yet on how players are sorted so I am just going to
	 * use {@link PlayerEloWritable#playerId}.
	 * 
	 * TODO Do we want to compare on {@link PlayerEloWritable#elo} so we can
	 * sort players at some point?
	 * 
	 * @return The result of {@link PlayerEloWritable#playerId}'s
	 *         {@link String#compareTo}
	 */
	@Override
	public int compareTo(PlayerEloWritable other) {

		if (other == null) {
			return -1;
		}

		return playerId.compareTo(other.getPlayerId());
	}

	/**
	 * This function compare equality. The only thing we care about when
	 * comparing players is if their ID's are equal. Should always return false
	 * if this {@link PlayerEloWritable#playerId} is equal to
	 * {@link Constants#INVALID_ID}.
	 * 
	 * @param o
	 *            The other object to compare to this one
	 * @return true if:
	 *         <ul>
	 *         <li>The given object is an {@link PlayerEloWritable} with a
	 *         {@link PlayerEloWritable#playerId} equal to this one</li>
	 *         <li>The given object is an {@link PlayerGameWritable} with a
	 *         {@link PlayerGameWritable#getPlayerId} equal to this
	 *         {@link PlayerEloWritable#playerId}</li>
	 *         <li>If it is a {@link String} equal to this
	 *         {@link PlayerEloWritable#playerId}
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

		if (o instanceof PlayerEloWritable) {

			PlayerEloWritable other = (PlayerEloWritable) o;
			return playerId.equals(other.getPlayerId());

		} else if (o instanceof PlayerGameWritable) {

			PlayerGameWritable other = (PlayerGameWritable) o;
			return playerId.equals(other.getPlayerId());

		} else if (o instanceof String) {

			String otherPlayerId = (String) o;
			return playerId.equals(otherPlayerId);

		} else {

			// Not one of the classes we were looking for
			return false;
		}
	}

	/**
	 * It is requried in Hadoop to return consistent hashcodes across instances
	 * of JVMs. Luckily for us {@link String#hashCode} already does this. This
	 * is also so when adding to HashSets or Hashmaps, we properly add or don't
	 * add.
	 * 
	 * @return the hashCode of {@link PlayerEloWritable#playerId}
	 */
	@Override
	public int hashCode() {
		return playerId.hashCode();
	}

}
