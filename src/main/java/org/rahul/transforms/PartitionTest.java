package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class PartitionTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8));

        PCollectionList<Integer> partitionedNumbers = numbers.apply(Partition.of(2,
                (Partition.PartitionFn<Integer>) (elem, numPartitions) -> elem % 2));

        PCollection<Integer> evenNumbers = partitionedNumbers.get(0);

        evenNumbers.apply(ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer element, OutputReceiver<Integer> out) {
                System.out.println(element);
                out.output(element);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
