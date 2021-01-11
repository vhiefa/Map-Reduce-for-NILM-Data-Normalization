package nilm;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ActivePowerNormalizationReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		//int frequencyForCountry = 0;
		double newActivePower;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			IntWritable value = (IntWritable) values.next();
			frequencyForCountry += value.get();
			newActivePower = (value.get() - minActivePower)/(maxActivePower-minActivePower);
			
		}
		output.collect(key, new IntWritable(frequencyForCountry));
	}
}
