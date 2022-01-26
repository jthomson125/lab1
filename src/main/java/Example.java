import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;
import java.util.List;

public class Example {
    public static void main(String[] args) {
        DataflowPipelineOptions pipelineOptions= PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("jt-labitsession2");
        pipelineOptions.setProject("york-cdf-start");
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setGcpTempLocation("gs://york_jimt_java/tmp");


        Pipeline pipeline = Pipeline.create(pipelineOptions);
        final List<String> input = Arrays.asList("PlayStation", "Gamera", "Darth Vader", "Spider-Man", "Kratos");
        pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://york_jimt_java/results/demo2").withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }
}
