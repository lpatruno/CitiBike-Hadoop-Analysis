import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *	Mapper to extract network information for the CitiBike data set
 *	Accepts data in default format (byteNo, line) and outputs
 *	date_startStationId_endStationId +-1
 *	The parity of 1 is chosen based on whether the trip starts (+1) or
 *	end (-1) at the startStation
 */
public class NetworkAnalysisMapper
   extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

		String line = value.toString();
                String[] fields = line.split(",");

		// Extract trip date, start_station_id and end_station_id
		String[] date_string = fields[1].replace("\"", "").split(" ");
		String trip_date = date_string[0];

		int start_id = Integer.parseInt(fields[3].replace("\"", ""));
		int end_id = Integer.parseInt(fields[7].replace("\"", ""));

		// Latitude and longitude coordinates for Map visualization
		String s_lat = fields[5].replace("\"", "");
		String s_lon = fields[6].replace("\"", "");
		String e_lat = fields[9].replace("\"", "");
		String e_lon = fields[10].replace("\"", "");

		String trip_key = "";
		int trip_value;

		// Form output key and value
		// key format: date_stationId_stationId
		// value 1 or -1
		// Do not count trips that start and end at the same location
		if (start_id < end_id){
			trip_key = trip_date + "_" + start_id + "_" + end_id + "_" + s_lat + "_" + s_lon + "_" + e_lat + "_" + e_lon;
			trip_value = 1;
			context.write(new Text(trip_key), new IntWritable(trip_value));
		} else if (start_id > end_id){
			trip_key = trip_date + "_" + end_id + "_" + start_id + "_" + e_lat + "_" + e_lon + "_" + s_lat + "_" + s_lon;
                        trip_value = -1;
			context.write(new Text(trip_key), new IntWritable(trip_value));
		}

                //context.write(new Text(trip_key), new IntWritable(trip_value));
        }

}

