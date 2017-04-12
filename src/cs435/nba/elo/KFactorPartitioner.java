package cs435.nba.elo;

import org.apache.hadoop.mapreduce.Partitioner;

public class KFactorPartitioner<K, V> extends Partitioner<KFactorDateWritable, V> {

	@Override
	public int getPartition(KFactorDateWritable key, V value, int numReduceTasks) {
		return key.getKFactor() % numReduceTasks;
	}

}
