// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

// vv OldMaxTemperature
public class Fill {
  
    static class NoaaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {

        private static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

            String line = value.toString();
            String[] data = line.split(",");
            String[] station_year = data[1].split("-");
            String estation = station_year[0] + "-" + station_year[1];
            String year = station_year[2];

            output.collect( new Text( estation ), new Text( year ) );


        }
    }
  
    static class NoaaReducer extends MapReduceBase
    implements Reducer <Text, Text, Text, Text> {
		
        int contador = 0;

        @Override
        public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {


        }
    }

 
}
// ^^ OldMaxTemperature
