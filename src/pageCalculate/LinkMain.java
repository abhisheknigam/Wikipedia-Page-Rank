package pageCalculate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import pageCalculate.LinkRank.CountMap;
import pageCalculate.LinkRank.CountReduce;
import pageCalculate.LinkRank.RankMap2;
import pageCalculate.LinkRank.CalculateRankMap;
import pageCalculate.LinkRank.RankReduce2;
import pageCalculate.LinkRank.CalculateRankReduce;
import pageCalculate.LinkRank.SortMapper;
import pageCalculate.LinkRank.SortReducer;


public class LinkMain {

    public static long count = 0;

    public static void main(String[] args) throws Exception {
        String input=args[0];
        String output=args[1];
        
        String results = output + "/";
        String temp = output + "/temp/";

        int numIterations = 8;
        String[] iterations = new String[numIterations+1];
        for ( int i =0; i <= numIterations ; i++){
            iterations[i] = "iter" + Integer.toString(i) + ".out";
        }
      
        String linkInput = input;
        LinkMain pageRank = new LinkMain();
        String num_nodes="num_nodes";
        pageRank.countNodes(linkInput, temp + num_nodes);
        pageRank.calculateRank(linkInput, temp + iterations[0], temp + num_nodes);
        
        for ( int i =1; i <= numIterations ; i++){
            pageRank.rank(temp + iterations[i-1], temp +iterations[i], temp + num_nodes);
        }
        
        String Iter1Sorted = "iter1.sort";
        String Iter8Sorted = "iter8.sort";
        
        pageRank.sort(temp + iterations[1], temp + Iter1Sorted, temp + num_nodes);
        pageRank.sort(temp + iterations[8], temp + Iter8Sorted, temp + num_nodes);

        pageRank.merge(linkInput, results + linkInput);
        pageRank.merge(temp + num_nodes, results + num_nodes);
        
        
        pageRank.merge(temp + Iter1Sorted, results + iterations[1]);
        pageRank.merge(temp + Iter8Sorted, results + iterations[8]);

    }

    public void countNodes(String input, String output) throws IOException{
        JobConf conf = new JobConf(LinkMain.class);
        conf.setJarByClass(LinkMain.class);
        
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(CountMap.class);
        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(CountReduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        JobClient.runJob(conf);
    }

    public void calculateRank (String input, String output, String linkcountfile) throws IOException {
        JobConf conf = new JobConf(LinkMain.class);
        conf.setJarByClass(LinkMain.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        
        conf.setMapperClass(CalculateRankMap.class);
        conf.setReducerClass(CalculateRankReduce.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Integer count = readCount(linkcountfile, conf);
        conf.set("count", Integer.toString(count));
        JobClient.runJob(conf);
    }


    public void rank(String input, String output, String linkcountfile) throws IOException{
        JobConf conf = new JobConf(LinkMain.class);
        conf.setJarByClass(LinkMain.class);
        
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        
        conf.setMapperClass(RankMap2.class);
        conf.setReducerClass(RankReduce2.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Integer count = readCount(linkcountfile, conf);
        conf.set("count", Integer.toString(count));
        JobClient.runJob(conf);
    }

    public void sort(String input, String output, String linkcountfile) throws IOException{
        JobConf conf = new JobConf(LinkMain.class);
        conf.setJarByClass(LinkMain.class);
        
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        
        conf.setMapperClass(SortMapper.class);
        conf.setReducerClass(SortReducer.class);

        conf.setMapOutputKeyClass(DoubleWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
        conf.setOutputKeyComparatorClass(KeyCompare.class);
        conf.setOutputValueGroupingComparator(Compare.class);
        conf.setPartitionerClass(FirstPartitioner.class);

        Integer count = readCount(linkcountfile, conf);
        conf.set("count", Integer.toString(count));
        JobClient.runJob(conf);
    }

    public static class FirstPartitioner implements Partitioner<DoubleWritable, Text> {
        @Override
        public void configure(JobConf job) {}

        @Override
        public int getPartition(DoubleWritable key, Text value, int numPartitions) {
            double d = (Double.parseDouble(key.toString()));
            int n =(int) d * 100;
            return (int)(n / numPartitions) ;
        }
    }

    public static class Compare extends WritableComparator {
        protected Compare() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable write1, WritableComparable write2) {
            DoubleWritable w1 = (DoubleWritable) write1;
            DoubleWritable w2 = (DoubleWritable) write2;
            return w1.compareTo(w2);
        }
    }

    public static class KeyCompare extends WritableComparator {
        protected KeyCompare() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable write1, WritableComparable write2) {
            DoubleWritable w1 = (DoubleWritable) write1;
            DoubleWritable w2 = (DoubleWritable) write2;
            
            int cmp = w1.compareTo(w2);
            return cmp * -1;
        }
    }

    private Integer readCount (String filepath, Configuration conf) throws IOException {
        String fileName = filepath + "/part-r-00";
        Integer count = 1;
        FileSystem fs = null;
        Path path ;
        NumberFormat nf = new DecimalFormat("000");
        Configuration config = new Configuration();
        try {
            path = new Path (fileName + nf.format(0));
            fs = path.getFileSystem(new Configuration());
            String line = "";
            if (!fs.isFile(path)){
                fileName = filepath + "/part-00";
            }
            line = readFile(fileName, nf, config, line);
            return Integer.parseInt(line);           
        } catch (IOException e) {
            e.printStackTrace();
        }       
        return count;
    }

    private void merge(String input, String output) throws  IOException {
        String fileName = input + "/part-r-00";
        NumberFormat nf = new DecimalFormat("000");
        FileSystem outFS = null;
        try {
            Path outFile = new Path(output);
            outFS = outFile.getFileSystem(new Configuration());
            if (outFS.exists(outFile)){
                System.out.println(outFile + " already exists");
                System.exit(1);
            }
            FSDataOutputStream out = outFS.create(outFile);
            Path inFile = new Path (fileName + nf.format(0));
            FileSystem inFS = inFile.getFileSystem(new Configuration());
            if (!inFS.exists(inFile)){
                fileName = input + "/part-00";
            }
            readMerge(fileName, nf, out);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                outFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String readFile(String fileName, NumberFormat nf, Configuration config, String line) throws IOException {
		BufferedReader br;
		FileSystem fs;
		Path path;
		for (int i=0; i<=999; i++){
		    path = new Path (fileName + nf.format(i));
		    fs = path.getFileSystem(config);
		    if (fs.isFile(path)){
		        br = new BufferedReader(new InputStreamReader(fs.open(path)));
		        line = br.readLine();

		        if (line!= null && !line.isEmpty() && line.length() >= 2)
		            break;
		    }
		}
		return line;
	}
    
	private void readMerge(String fileName, NumberFormat nf, FSDataOutputStream out) throws IOException {
		Path inFile;
		FileSystem inFS;
		for (int i=0; i<=999; i++){
		    inFile = new Path (fileName + nf.format(i));
		    inFS = inFile.getFileSystem(new Configuration());
		    if (inFS.isFile(inFile)){
		        int bytesRead=0;
		        byte[] buffer = new byte[4096];
		        FSDataInputStream in = inFS.open(inFile);
		        while ((bytesRead = in.read(buffer)) > 0) {
		            out.write(buffer, 0, bytesRead);
		        }
		        in.close();
		    }else{
		        break;
		    }
		    inFS.close();
		}
	}
  }