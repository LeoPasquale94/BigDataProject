package map_reduce.job1;

import map_reduce.Utils.Avg;
import map_reduce.Utils.Comparator;
import map_reduce.Utils.ReviewOrBrand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

            /* *****************************************************************
              Top N Video Games ordinati per media voto più alta sulle loro recensioni,
                        create da almeno M recensori stilare la classifica per brand o categoria.
             ******************************************************************/

public class MainJob1 implements Tool {

    private Configuration conf;
    private List<Path> paths = new ArrayList<>();


    public static void main(String[] args) throws Exception{

        ToolRunner.run(new Configuration(), new MainJob1(), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        setPaths(args, fs);

        //Conf Review Avg
        Job job1 = new Job(getConf());
        job1.setJobName("Job1 - Join");
        job1.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

        job1.setJarByClass(MainJob1.class);
        job1.setReducerClass(AvgReviewReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(ReviewOrBrand.class);

        MultipleInputs.addInputPath(job1, getFirstDSPath(), TextInputFormat.class, JoinOverallVideoGameMapper.class);
        MultipleInputs.addInputPath(job1, getSecondDSPath(), TextInputFormat.class, JoinBrandVideoGameMapper.class);

        FileOutputFormat.setOutputPath(job1, paths.get(2));
        job1.waitForCompletion(true);

        //Conf Brand Avg
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job1 - Brand Avg");

        job2.setJarByClass(MainJob1.class);
        job2.setMapperClass(AvgBrandMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Avg.class);

        job2.setReducerClass(AvgBrandReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Avg.class);

        FileInputFormat.addInputPath(job2, paths.get(2));
        FileOutputFormat.setOutputPath(job2, paths.get(3));

        job2.waitForCompletion(true);

        //Conf Sort
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job1 - Sort");

        job3.setJarByClass(MainJob1.class);
        job3.setMapperClass(SortMapper.class);
        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(SortReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setSortComparatorClass(Comparator.class);

        FileInputFormat.addInputPath(job3,paths.get(3));
        FileOutputFormat.setOutputPath(job3, getFinalPath());

        return job3.waitForCompletion(true) ? 0 : 1;

    }

    public void setPaths(String[] args, FileSystem fs ) throws IOException {
        paths.add(new Path(args[0]));
        paths.add(new Path(args[1]));
        for(int i=1; i<3; i++){
            paths.add(new Path(args[2] + "/temp" + i ));
            existsFold(paths.get(paths.size() - 1),fs);
        }

        paths.add(new Path( args[2] + "/final"));
        existsFold(paths.get(paths.size() - 1),fs);
    }

    private Path getFirstDSPath(){
        return paths.get(0);
    }

    private Path getSecondDSPath(){
        return paths.get(1);
    }

    private Path getFinalPath(){
        return paths.get(paths.size() - 1);
    }

    private void existsFold(Path path, FileSystem fs) throws IOException {
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
