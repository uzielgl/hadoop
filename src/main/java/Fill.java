// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

// vv OldMaxTemperature
public class Fill {
  
    static class FillMapper extends MapReduceBase
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
			
			//Mandamos año y un iterable de [ciudad,mes,temperatura,fecha]
			output.collect( new Text( year ),
				new Text( country + "," + date_temp ) ) ;
        }
    }
  
    static class FillReducer extends MapReduceBase
    implements Reducer <Text, Text, Text, Text> {
		int c = 0;
		
        @Override
        public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
		
			String line = "";
			String header = "";
			TreeMap<String, String> ciudades = new TreeMap<String, String>();	
			
			//Obtenemos los datos y los metemos a un treemap para que los ordene por ciudad
			while( values.hasNext() ){
				String[] tmp = values.next().toString().split(",");
				String ciudad = tmp[0];
				String mes = tmp[1];
				String temperatura = tmp[2];
				String fecha = tmp[3];
				
				ciudades.put( ciudad, tmp[1] + "," + tmp[2] + "," + tmp[3] );
			}
			
			//Recorremos las ciudades y vamos imprimiendo
			for( String ciudad: ciudades.keySet() ){
				header += ciudad + ",,";
				String[] temporal = ciudades.get( ciudad ).split(",");
				line += temporal[2] + "," + temporal[1] + ",";
			}
	
			if( c == 0){ //Imprimimos cabezera
				output.collect( new Text( "Año," ), new Text( header ) );
				c++;
			}
			
			output.collect( new Text( key.toString() + "," ),
				new Text( line ) );
        }
    }

 
}
// ^^ OldMaxTemperature
