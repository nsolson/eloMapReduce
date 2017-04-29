package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PlayerEloSalaryWritable implements WritableComparable<PlayerEloSalaryWritable> {

	private String playerId;
	private String name;
	private String team;
	private double elo;
	private double salary;

	/**
	 * Required for hadoop
	 */
	public PlayerEloSalaryWritable() {
		this(Constants.EMPTY_STRING, Constants.EMPTY_STRING, Constants.EMPTY_STRING, Constants.INVALID_STAT,
				Constants.INVALID_STAT);
	}

	public PlayerEloSalaryWritable(String playerId, String name, String team, double elo, double salary) {
		this.playerId = playerId;
		this.name = name;
		this.team = team;
		this.elo = elo;
		this.salary = salary;
	}

	public String getPlayerId() {
		return playerId;
	}

	public String getName() {
		return name;
	}

	public String getTeam() {
		return team;
	}

	public double getElo() {
		return elo;
	}

	public double getSalary() {
		return salary;
	}

	public double getSalaryInMillions() {
		return salary / 1000000;
	}

	public double getEloPerMillion() {

		if (salary != 0 && salary != Constants.INVALID_STAT) {
			return elo / getSalaryInMillions();
		} else {
			return 0;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		playerId = WritableUtils.readString(in);
		name = WritableUtils.readString(in);
		team = WritableUtils.readString(in);
		elo = Double.parseDouble(WritableUtils.readString(in));
		salary = Double.parseDouble(WritableUtils.readString(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, playerId);
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, team);
		WritableUtils.writeString(out, Double.toString(elo));
		WritableUtils.writeString(out, Double.toString(salary));
	}

	@Override
	public int compareTo(PlayerEloSalaryWritable other) {

		// Want the highest elo per dollar to be "less than"
		if (other == null) {
			return -1;
		}

		double thisEloPerMillion = this.getEloPerMillion();
		double otherEloPerMillion = other.getEloPerMillion();

		if (thisEloPerMillion > otherEloPerMillion) {
			return -1;
		} else if (otherEloPerMillion > thisEloPerMillion) {
			return 1;
		} else {

			// if elo per dollar equal, make highest elo "less than"
			double otherElo = other.getElo();

			if (elo > otherElo) {
				return -1;
			} else if (otherElo > elo) {
				return 1;
			} else {

				// all equal
				return 0;
			}
		}
	}

	@Override
	public String toString() {
		return name + "\t" + team + "\t" + getEloPerMillion() + "\t" + elo + "\t" + salary;
	}

}
