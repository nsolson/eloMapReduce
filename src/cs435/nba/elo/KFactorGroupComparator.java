package cs435.nba.elo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KFactorGroupComparator extends WritableComparator {

	public KFactorGroupComparator() {
		super(KFactorDateWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable one, WritableComparable two) {

		KFactorDateWritable kfOne = (KFactorDateWritable) one;
		KFactorDateWritable kfTwo = (KFactorDateWritable) two;

		int kFactorOne = kfOne.getKFactor();
		int kFactorTwo = kfTwo.getKFactor();

		return kFactorOne - kFactorTwo;
	}
}
