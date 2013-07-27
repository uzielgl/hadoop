// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

// vv OldMaxTemperature
public class Pivot {
  
    static class PivotMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {

        private static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

            String line = value.toString();
            StringTokenizer tokens = new StringTokenizer( line );
			
			String[] keys = tokens.nextToken().toString().split("-");
			String date_temp = tokens.nextToken();
			
			String country = keys[0];
			String year = keys[1];
			String month = keys[2];
			
			//Mandamos pais-año y un iterable de [fechas,temperatura,mes]
			
			output.collect( new Text( country + "-" + year ),
				new Text( date_temp + "," + month ) ) ;
        }
    }
  
    static class PivotReducer extends MapReduceBase
    implements Reducer <Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
		
			//Calculamos la temperatura mayor
			float max_temp = Float.MIN_VALUE;
			String max_month = "";
			String max_date = "";
			while( values.hasNext() ){
				String[] tmp = values.next().toString().split(",");
				String date = tmp[0];
				float temp = Float.parseFloat( tmp[1] );
				String month = tmp[2];
				
				if( temp > max_temp ){
					max_month = month;
					max_temp = temp;
					max_date = date;
				}
			}
			
			String s_max_temp = String.format("%2.02f", max_temp);
			
			output.collect( key, new Text( max_month + "," + s_max_temp + "," + max_date ) );

        }
    }

 
}
// ^^ OldMaxTemperature
