// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

// vv OldMaxTemperature
public class NoaaMonterrey {

	public static String ciudad = "monterrey";
	
	private static final int MISSING = 9999;
	
    static class NoaaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
            try{
                String line = value.toString();
                String year = line.substring(15, 19) ;
                String month =  line.substring(19, 21) ;
				String day =  line.substring(21, 23) ;
                int airTemperature;
                if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
                  airTemperature = Integer.parseInt(line.substring(88, 92));
                } else {
                  airTemperature = Integer.parseInt(line.substring(87, 92));
                }
				
				String quality = line.substring(92, 93);
				if (airTemperature != MISSING && quality.matches("[01459]")) {
					output.collect( new Text( ""+year+"-"+month ), 
						new Text( day + "/" + month + "/" + year + "@" + airTemperature ) );
				}

                
            }catch( Exception e){
                
            }
        }
    }
  
    static class NoaaReducer extends MapReduceBase
    implements Reducer <Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
            
			String date_max = "";
			int max_temp = Integer.MIN_VALUE;
			
            while( values.hasNext() ){
				String[] date_temp = values.next().toString().split("@");
				String date = date_temp[0];
				int temp = Integer.parseInt( date_temp[1] );
				if( temp > max_temp){
					max_temp = temp;
					date_max = date;
				}
            }
			
			float air_temp = ( (float) max_temp ) / 10;
			String air_temp_str = String.format("%2.02f", air_temp);
            
            output.collect( new Text( ciudad + "-" + key),  new Text( date_max + "," + air_temp_str ));
            //output.collect( new Text( ciudad + "-" + key), new Text( max_month + "," + max_tmp ) );
        }
    }

 
}
// ^^ OldMaxTemperature
