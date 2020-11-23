package map_reduce.job2;

import map_reduce.job1.JoinOverallVideoGameMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 /* ******************************************************************************
 JOB-2) Classificazione dei Video Games in base alla media voto delle recensioni e
        numero di recensioni per classe di voto:

                 0 <= media voto <= 3: interesse basso;
                 3 < media voto < 4: interesse medio;
                 4 <= media voto <= 5: interesse alto;

 *********************************************************************************/

public class Job2 {

    public static void main(String[] args) throws Exception {
        //JOB 2.1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "job2.1");
        FileSystem fs = FileSystem.get(new Configuration());
        Path startInputPath = new Path(args[0]),
                middleOutputPath = new Path(args[1]),
                finalOutputPath = new Path(args[2]);

        if (fs.exists(middleOutputPath)) {
            fs.delete(middleOutputPath, true);
        }

        job1.setJarByClass(Job2.class);
        job1.setMapperClass(JoinOverallVideoGameMapper.class);
        job1.setReducerClass(AverageReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, startInputPath);
        FileOutputFormat.setOutputPath(job1, middleOutputPath);
        job1.waitForCompletion(true);

        //JOB 2.2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "job2.2");
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }
        job2.setJarByClass(Job2.class);

        job2.setMapperClass(ClassifierMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(ReviewsClassificationReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, middleOutputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}