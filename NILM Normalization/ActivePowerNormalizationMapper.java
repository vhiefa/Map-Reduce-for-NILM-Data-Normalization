package nilm;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class NormalizationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString(); //one line/row data in Spring
		String[] SingleData = valueString.split(","); //split that one line data by comma into array of string
		output.collect(new Text(SingleData[9]), one); //activePower data (array no 9)
	}
}
