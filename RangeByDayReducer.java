import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *	Mapper to extract the top 5 CitiBike trips with
 *      the largest range between trips from a startion location
 *      to another location (they could be the same location).Accepts 
 *      data in format 'date startId_endId_range', adds the trip info to a 
 *	hashmap in the form K,V = startId_endId_range,absolute(range), 
 *	sorts these in descending order and then prints out the top 5.
 */
public class RangeByDayReducer
  extends Reducer<Text, Text, Text, Text> {

	/**
 	 *	Extract the top ranges.
 	 */ 
	private static final int TOP = 5;

	/**
 	 *	Sort the map in descending order. 
 	 */ 
	private static final boolean DESC = false;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	  throws IOException, InterruptedException {
		
		// Unsorted data
		Map<String, Integer> unsortedMap = new HashMap<String, Integer>();
		
		// Counter to extract top values
		int count = 0;
	
		for (Text value : values) {

			String[] info = value.toString().split("_");
			int range = Math.abs(Integer.parseInt(info[6]));
			
			unsortedMap.put(value.toString(), range);
		}

		Map<String, Integer> sortedMap = sortByComparator(unsortedMap, DESC);

		for(Entry<String, Integer> entry: sortedMap.entrySet()){
			if (count < TOP){
				String output = key + "," + entry.getKey();
				context.write(new Text(output), new Text(""));
				count++;
			}		
		}
	}

	/**
 	 *	Method to sort a map by integr value
 	 *	@param unsortMap the unsorted map to sort
 	 *	@param order True for ascending order, False for descending order
 	 */ 
	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order){

	        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        	// Sorting the list based on values
        	Collections.sort(list, new Comparator<Entry<String, Integer>>(){
            		public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2){
                		if (order){
                    			return o1.getValue().compareTo(o2.getValue());
                		} else {
                    			return o2.getValue().compareTo(o1.getValue());
				}
            		}
        	});

        	// Maintaining insertion order with the help of LinkedList
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        	for (Entry<String, Integer> entry : list) {
            		sortedMap.put(entry.getKey(), entry.getValue());
        	}

        	return sortedMap;
    	}
}
