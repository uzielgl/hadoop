// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

// vv OldMaxTemperature
public class Oxford{

	public static String ciudad = "oxford";
	
	private static final int MISSING = 9999;
	
    static class NoaaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
			String line = value.toString();
			try{
				String year = line.substring(3, 7);
				StringTokenizer tokens = new StringTokenizer( line );
				
				int int_year = Integer.parseInt( tokens.nextToken() );
				String month = String.format( "%02d", Integer.parseInt( tokens.nextToken() ) );
				String temp_max = tokens.nextToken();
				
				if( int_year >= 2008 && int_year <= 2012 ){
					output.collect( new Text( year + "-" + month ), 
						new Text( "dd/" + month + "/" + year + "@" + temp_max ) ); 
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
            String value = values.next().toString();
			value = value.replace("@", "," );
			value = value.replace("*", "" );
			
			output.collect( new Text( ciudad + "-" + key),  new Text( value ) );
            //output.collect( new Text( ciudad + "-" + key), new Text( max_month + "," + max_tmp ) );
        }
    }

 
}
// ^^ OldMaxTemperature
