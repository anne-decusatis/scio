import com.spotify.scio.avro._
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.Gen

class JobTest extends PipelineSpec {
  /*
  [info] JobTest:
[info] Job
[info] - should not throw an exception because it cannot encode a null Avro value *** FAILED *** (6 seconds, 225 milliseconds)
[info]   org.apache.beam.sdk.Pipeline$PipelineExecutionException: com.spotify.scio.coders.CoderException: Exception while trying to `encode` an instance of Tuple2: Can't decode field _2
[info]   at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:332)
[info]   at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:302)
[info]   at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:197)
[info]   at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:64)
[info]   at org.apache.beam.sdk.Pipeline.run(Pipeline.java:313)
[info]   at org.apache.beam.sdk.testing.TestPipeline.run(TestPipeline.java:350)
[info]   at org.apache.beam.sdk.testing.TestPipeline.run(TestPipeline.java:331)
[info]   at com.spotify.scio.ScioContext$$anonfun$close$1.apply(ScioContext.scala:537)
[info]   at com.spotify.scio.ScioContext$$anonfun$close$1.apply(ScioContext.scala:525)
[info]   at com.spotify.scio.ScioContext.requireNotClosed(ScioContext.scala:557)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException: Exception while trying to `encode` an instance of Tuple2: Can't decode field _2
[info]   at com.spotify.scio.coders.instances.PairCoder.onErrorMsg(ScalaCoders.scala:56)
[info]   at com.spotify.scio.coders.instances.PairCoder.encode(ScalaCoders.scala:61)
[info]   at com.spotify.scio.coders.instances.PairCoder.encode(ScalaCoders.scala:49)
[info]   at org.apache.beam.sdk.coders.Coder.encode(Coder.java:136)
[info]   at org.apache.beam.sdk.coders.NullableCoder.encode(NullableCoder.java:73)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$2.apply$mcV$sp(Coder.scala:204)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$2.apply(Coder.scala:204)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$2.apply(Coder.scala:204)
[info]   at com.spotify.scio.coders.WrappedBCoder.catching(Coder.scala:189)
[info]   at com.spotify.scio.coders.WrappedBCoder.encode(Coder.scala:204)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:46)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:27)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo(PCollectionWrapper.scala:55)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo$(PCollectionWrapper.scala:52)
[info]   at com.spotify.scio.values.SCollectionImpl.parDo(SCollection.scala:1199)
[info]   at com.spotify.scio.values.SCollection.map(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollection.map$(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollectionImpl.map(SCollection.scala:1199)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException: cannot encode a null value
[info]   at com.spotify.scio.coders.KryoAtomicCoder$$anonfun$encode$1.apply(KryoAtomicCoder.scala:127)
[info]   at com.spotify.scio.coders.KryoAtomicCoder$$anonfun$encode$1.apply(KryoAtomicCoder.scala:125)
[info]   at com.spotify.scio.coders.KryoAtomicCoder$.withKryoState(KryoAtomicCoder.scala:263)
[info]   at com.spotify.scio.coders.KryoAtomicCoder.encode(KryoAtomicCoder.scala:125)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$1.apply$mcV$sp(Coder.scala:195)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$1.apply(Coder.scala:195)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$1.apply(Coder.scala:195)
[info]   at com.spotify.scio.coders.WrappedBCoder.catching(Coder.scala:189)
[info]   at com.spotify.scio.coders.WrappedBCoder.encode(Coder.scala:195)
[info]   at com.spotify.scio.coders.instances.PairCoder$$anonfun$encode$4.apply$mcV$sp(ScalaCoders.scala:61)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:48)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:50)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:27)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo(PCollectionWrapper.scala:55)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo$(PCollectionWrapper.scala:52)
[info]   at com.spotify.scio.values.SCollectionImpl.parDo(SCollection.scala:1199)
[info]   at com.spotify.scio.values.SCollection.map(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollection.map$(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollectionImpl.map(SCollection.scala:1199)
[info]   ...

   */

  def newRecord(): GenericRecord = {
    val r = TestRecord
      .newBuilder()
      .setStringField(null)
      .build()
    r
  }

  "Job" should "not throw an exception because it cannot encode a null Avro value" in {

    val record: GenericRecord = newRecord()

    val recordSeq = Seq(record)

    JobTest[Job.type]
      .args(
        "--input=gs://testing.in",
        "--output=gs://testing.out",
        "--projectId=ratatool-playground",
        "--nullableCoders=true"
      )
      .input(AvroIO[GenericRecord]("gs://testing.in"), recordSeq)
      .output(AvroIO[GenericRecord]("gs://testing.out"))(_ should containSingleValue(record))
      .run()
  }

}
