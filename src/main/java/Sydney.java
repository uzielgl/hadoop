// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

// vv OldMaxTemperature
public class Sydney{

	public static String del2last( String c ){
		return c.substring( 0, c.length() - 2 );
	}

	public static String ciudad = "sydney";
	
	private static final int MISSING = 9999;
	
    static class NoaaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
			String line = value.toString();
			try{
				StringTokenizer tokens = new StringTokenizer( line );
				String year = tokens.nextToken();
				
				//Está instrucción si viene un cadena saltará al catch, y no  hará nada
				int int_year = Integer.parseInt( year );
				
				String temp_max = "";
				String[] months = { "01", "02", "03", "04", "05", "06", "07","08","09", "10", "11", "12"};
				if( int_year >= 2008 && int_year <= 2012 ){
					for( String month: months ){
						temp_max = del2last( tokens.nextToken().toString() );
						output.collect( new Text( year + "-" + month ),
							new Text( "dd/" + month + "/" + year + "@" + temp_max) );
					}
				}
			}catch(Exception e){
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
			
			output.collect( new Text( ciudad + "-" + key),  new Text( value ) );
            //output.collect( new Text( ciudad + "-" + key), new Text( max_month + "," + max_tmp ) );
        }
    }

 
}
// ^^ OldMaxTemperature
