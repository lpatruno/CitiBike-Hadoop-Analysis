import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *	Reducer which outputs the startId, endId and number of trips
 *	between each station.The first number is number of trips from
 *	start station to end station and the second number is the number of reverse trips
 */
public class NetworkAnalysisReducer
  extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	  throws IOException, InterruptedException {

		// Number of trips from start station to end station
		int forward = 0;
		// Number of trips from end station to start stations
		int backward = 0;
	
		for (IntWritable value : values) {
			if(value.get() > 0){
				forward++;
			} else {
				backward++;
			}	
		}

		String output = "" + forward + " " + backward;

		context.write(key, new Text(output));
	}
}
