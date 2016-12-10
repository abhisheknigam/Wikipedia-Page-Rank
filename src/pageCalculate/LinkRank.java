package pageCalculate;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LinkRank {

	public static class CountMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, NullWritable> {

	    public void map(LongWritable key, Text value, OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException {
	        final LongWritable initialize = new LongWritable(1);
	        output.collect(initialize, NullWritable.get());
	    }
	}
	
	public static class CountReduce extends MapReduceBase implements Reducer<LongWritable, NullWritable, Text, NullWritable> {
	    public void reduce(LongWritable key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
	        long count = 0;
	        while(values.hasNext()){
	            values.next();
	            count++;
	        }
	        output.collect(new Text(Long.toString(count)), NullWritable.get());
	    }
	}
	
	public static class CalculateRankMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    public static String count;

	    public void configure(JobConf jobConf){
	        count  = jobConf.get("count");
	    }

	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        String val = value.toString();
	        int last = val.indexOf("\t");
	        String tiIndex = val.substring(0,last);
	        String links = val.substring(last + 1);
	        output.collect(new Text(tiIndex), new Text(links));
	    }
	}
	

	public static class CalculateRankReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    public static String count;

	    public void configure(JobConf jobConf){
	        count  = jobConf.get("count");
	    }
	    
	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        double ranks = 1.0 / Integer.parseInt(count);
	        String cal="";
	        while (values.hasNext()){
	            cal = values.next().toString();
	        }
	        output.collect(key, new Text(Double.toString(ranks) + "\t" + cal) );
	    }
	}
	
	public static class RankMap2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        String cal = value.toString();
	        String[] parts = cal.split("\t");
	        String title = parts[0];
	        String rank = parts[1];

	        String outlinks = "";
	        String ranks = "";
	        int linkCount = 0;
	        double increment=0.0;
	        
	        if ( parts.length > 2){
	            int titleEndIndex = cal.indexOf("\t");
	            int rankEndIndex = cal.indexOf("\t",titleEndIndex+1);
	            outlinks = cal.substring(rankEndIndex+1);
	            linkCount = parts.length - 2;
	            ranks += "\t" + outlinks;
	            increment = Double.parseDouble(rank)/linkCount;
	        }
	        collectOutputs(output, parts, title, ranks, increment);
	    }

		private void collectOutputs(OutputCollector<Text, Text> output, String[] parts, String title,
				String rankOutlinks, double rankVote) throws IOException {
			output.collect(new Text(title), new Text("$\t"));
	        if ( rankOutlinks.equals("") ){
	            output.collect(new Text(title), new Text("#"));
	        }else{
	            output.collect(new Text(title), new Text("#" + rankOutlinks));
	        }
	        for(int j=2; j < parts.length ; j++){
	            output.collect(new Text(parts[j]), new Text(Double.toString(rankVote)));
	        }
		}
	}

	
	public static class RankReduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    public static String count;
	    public void configure(JobConf jobConf){
	        count  = jobConf.get("count");
	    }

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	        int N = Integer.parseInt(count);
	        String title = key.toString();
	        double rank = 0.0;
	        double votes = 0.0;
	        double d = 0.85f;
	        String links = "";
	       
	        boolean noOutLinks = false;
	        boolean reds=true;
	        while(values.hasNext()){
	            String val = values.next().toString();
	            if ( val.indexOf("\t") != -1){
	                String[] cal = val.split("\t");
	                if (cal[0].equals("#")){
	                    for(int i= 1; i < cal.length; i++){
	                        links += "\t" + cal[i];
	                    }
	                    continue;
	                }
	                if (cal[0].equals("$")){
	                    reds = false;
	                    continue;
	                }
	            }else{
	                if (val.equals("#")){
	                    noOutLinks = true;
	                }else{
	                    votes += Double.parseDouble(val);
	                }
	            }
	        }
	        if (!reds){
	            rank = (0.15/N) + d * votes ;
	            if (noOutLinks){
	                output.collect(new Text(title), new Text ( Double.toString(rank)  ) );
	            }else{ 
	                output.collect(new Text(title), new Text ( Double.toString(rank) + links ) );
	            }
	        }
	    }
	}
	
	
	public static class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

	    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
	        String val = value.toString();
	        int last = val.indexOf("\t");
	        int end = val.indexOf( "\t", last+1);

	        if ( end == -1){
	            end = val.length();
	        }
	        String srank = val.substring(last+1,end);
	        output.collect(new DoubleWritable(Double.parseDouble(srank)), new Text(val.substring(0,last)));
	    }
	}
	    
	public static class SortReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	    public static String count="1.0";
	    public void configure(JobConf jobConf){
	        count  = jobConf.get("count");
	    }
	    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	        int N = Integer.parseInt(count);
	        double threshhold = 5.0/N;

	        while(values.hasNext()){
	            double result = Double.parseDouble(key.toString());
	            if (result >  threshhold)
	                output.collect(values.next(), key);
	            else
	               return;
	        }
	    }
	}
}

/**
* Reference MapReduce GIT, MapReduce PageRank Calculation
*/