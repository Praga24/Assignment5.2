import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AssignMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable Key, Text Value, Context context) throws IOException, InterruptedException {
		String[] data = Value.toString().split(Pattern.quote("|"));
		if (!data[0].equals("NA")) {
			Text comp_name = new Text(data[0]);
			IntWritable one = new IntWritable(1);
			context.write(comp_name, one);
		}
	}

}
