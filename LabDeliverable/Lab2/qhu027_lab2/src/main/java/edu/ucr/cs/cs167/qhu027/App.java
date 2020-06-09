package edu.ucr.cs.cs167.qhu027;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static java.lang.Math.pow;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        if(args.length != 2){
            System.out.println("Amount of command line arguments is incorrect");
            System.exit(-1);
        }
        // Store the two arguments in local variables of type org.apache.hadoop.fs.Path
        Path inputfile = new Path(args[0]);
        Path outputfile = new Path(args[1]);
        // Retrieve the correct file system for the two files and
        // store in a variable of type org.apache.hadoop.fs.FileSystem
        Configuration conf = new Configuration();
        FileSystem fs1 = inputfile.getFileSystem(conf);
        FileSystem fs2 = outputfile.getFileSystem(conf);
        // Check whether the input file exists or not. If it does not exist, write an error message and exit.
        if(!fs1.exists(inputfile))
        {
            System.out.println("Input file does not exist");
            return;
        }
        // check whether the output file exists or not. If it already exists, write an error message and exit.
        if(fs2.exists(outputfile))
        {
            System.out.println("Output file already created");
            return;
        }
        FSDataInputStream inputStream = fs1.open(inputfile);
        FSDataOutputStream outputStream = fs2.create(outputfile);

        long timer_start = System.nanoTime(); // Timing start
        byte[] buf = new byte[1024];
        int len;
        long total=0;
        while((len = inputStream.read(buf)) > 0) // Read and copy
        {
            outputStream.write(buf, 0, len);
            total += len;
        }
        double timer_end = System.nanoTime(); // Timing end
        double delta_time = (timer_end - timer_start) / pow(10,9);

        outputStream.close();
        inputStream.close();

        System.out.println("From file ["+ inputfile+"] to file ["+outputfile+"], bytes="+total+", time="+delta_time + " second(s)");




    }
}
