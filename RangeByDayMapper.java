import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *	Mapper to extract the top 5 CitiBike trips with
 *	the largest range between trips from a startion location
 *	to another location (they could be the same location).Accepts 
 *	key value pairs from previous MapReduce output and itself
 *	outputs 'date startId_endId_range'.	
 */
public class RangeByDayMapper
   extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context)
          throws IOException, InterruptedException {

		String[] key_info = key.toString().split("_");
		
		String date = key_info[0];
		String start_id = key_info[1];
		String end_id = key_info[2];
		String s_lat = key_info[3];
		String s_lon = key_info[4];
		String e_lat = key_info[5];
		String e_lon = key_info[6];

		String[] num_trips = value.toString().split(" ");
		
		int forward = Integer.parseInt(num_trips[0]);
		int backward = Integer.parseInt(num_trips[1]);
		int range = forward - backward;

		String output_value = start_id + "_" + end_id + "_" + s_lat + "_" + s_lon + "_" + e_lat + "_" + e_lon + "_" + range;
 
                context.write(new Text(date), new Text(output_value));
        }

}

