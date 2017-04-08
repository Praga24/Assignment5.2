import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AssignPartitioner extends Partitioner<Text, IntWritable> {

	private static final String Pattern1 = "ABCDEF";
	private static final String Pattern2 = "GHIJKL";
	private static final String Pattern3 = "MNOPQR";
	
	@Override
	public int getPartition(Text key, IntWritable value, int arg2) {
	
		String k = key.toString().toUpperCase().substring(0, 1);
		if (Pattern1.contains(k) ) 
		{		
				return 0;
		}
		else if (Pattern2.contains(k))
		{
			return 1;
		}
		else if (Pattern3.contains(k))
		{
				return 2;
		}
		else
		{
			return 3;
		}
	
	}

}