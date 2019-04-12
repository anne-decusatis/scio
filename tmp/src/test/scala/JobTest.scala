import com.spotify.scio.avro._
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.Gen
import scala.collection.JavaConverters._

class JobTest extends PipelineSpec {

  def newRecord(): GenericRecord = {
    val nts = NestedTestString.newBuilder().setStringField(null).build()
    val r = NestedTestRecord
      .newBuilder()
      .setNested(nts)
      .setRepeatedNestedField(Seq(nts).asJava)
      .build()
//    val r = TestRecord.newBuilder().setStringField(null).build()
    r
  }

  "Job" should "not throw an exception because it cannot encode a null Avro value" in {

    /*
    This job throws this exception:

    [pool-142-thread-6-ScalaTest-running-JobTest] WARN com.spotify.scio.VersionUtil$ - Using a SNAPSHOT version of Scio: 0.7.5-SNAPSHOT
[info] JobTest:
[info] Job
[info] - should not throw an exception because it cannot encode a null Avro value *** FAILED *** (5 seconds, 608 milliseconds)
[info]   org.apache.beam.sdk.Pipeline$PipelineExecutionException: com.spotify.scio.coders.CoderException: Exception while trying to `decode` an instance of Tuple2: Can't decode field _2
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
[info]   Cause: com.spotify.scio.coders.CoderException: Exception while trying to `decode` an instance of Tuple2: Can't decode field _2
[info]   at com.spotify.scio.coders.instances.PairCoder.onErrorMsg(ScalaCoders.scala:56)
[info]   at com.spotify.scio.coders.instances.PairCoder.decode(ScalaCoders.scala:65)
[info]   at com.spotify.scio.coders.instances.PairCoder.decode(ScalaCoders.scala:49)
[info]   at org.apache.beam.sdk.coders.Coder.decode(Coder.java:159)
[info]   at org.apache.beam.sdk.coders.NullableCoder.decode(NullableCoder.java:94)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$decode$2.apply(Coder.scala:201)
[info]   at com.spotify.scio.coders.WrappedBCoder.catching(Coder.scala:189)
[info]   at com.spotify.scio.coders.WrappedBCoder.decode(Coder.scala:201)
[info]   at org.apache.beam.sdk.util.CoderUtils.decodeFromSafeStream(CoderUtils.java:115)
[info]   at org.apache.beam.sdk.util.CoderUtils.decodeFromByteArray(CoderUtils.java:98)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:46)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:27)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo(PCollectionWrapper.scala:55)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo$(PCollectionWrapper.scala:52)
[info]   at com.spotify.scio.values.SCollectionImpl.parDo(SCollection.scala:1199)
[info]   at com.spotify.scio.values.SCollection.map(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollection.map$(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollectionImpl.map(SCollection.scala:1199)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException:
[info]   at org.apache.avro.generic.GenericData$Array.add(GenericData.java:277)
[info]   at com.esotericsoftware.kryo.serializers.CollectionSerializer.read(CollectionSerializer.java:134)
[info]   at com.esotericsoftware.kryo.serializers.CollectionSerializer.read(CollectionSerializer.java:40)
[info]   at com.esotericsoftware.kryo.Kryo.readClassAndObject(Kryo.java:813)
[info]   at com.spotify.scio.coders.KryoAtomicCoder$$anonfun$decode$1.apply(KryoAtomicCoder.scala:155)
[info]   at com.spotify.scio.coders.KryoAtomicCoder$$anonfun$decode$1.apply(KryoAtomicCoder.scala:150)
[info]   at com.spotify.scio.coders.KryoAtomicCoder$.withKryoState(KryoAtomicCoder.scala:263)
[info]   at com.spotify.scio.coders.KryoAtomicCoder.decode(KryoAtomicCoder.scala:150)
[info]   at org.apache.beam.sdk.coders.Coder.decode(Coder.java:159)
[info]   at org.apache.beam.sdk.coders.NullableCoder.decode(NullableCoder.java:94)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:49)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:27)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo(PCollectionWrapper.scala:55)
[info]   at com.spotify.scio.values.PCollectionWrapper.parDo$(PCollectionWrapper.scala:52)
[info]   at com.spotify.scio.values.SCollectionImpl.parDo(SCollection.scala:1199)
[info]   at com.spotify.scio.values.SCollection.map(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollection.map$(SCollection.scala:488)
[info]   at com.spotify.scio.values.SCollectionImpl.map(SCollection.scala:1199)
[info]   ...

     */

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

/*
This is the exception my 'real' job throws and I don't know if they have the same cause:
[info] BigSamplerJobTest:
[info] BigSampler
[info] - should not throw an exception because it cannot encode a null Avro value *** FAILED ***
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
[info]   at com.spotify.scio.coders.instances.PairCoder.onErrorMsg(ScalaCoders.scala:54)
[info]   at com.spotify.scio.coders.instances.PairCoder.encode(ScalaCoders.scala:61)
[info]   at com.spotify.scio.coders.instances.PairCoder.encode(ScalaCoders.scala:49)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$1.apply$mcV$sp(Coder.scala:195)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$1.apply(Coder.scala:195)
[info]   at com.spotify.scio.coders.WrappedBCoder$$anonfun$encode$1.apply(Coder.scala:195)
[info]   at com.spotify.scio.coders.WrappedBCoder.catching(Coder.scala:189)
[info]   at com.spotify.scio.coders.WrappedBCoder.encode(Coder.scala:195)
[info]   at org.apache.beam.sdk.coders.IterableLikeCoder.encode(IterableLikeCoder.java:98)
[info]   at org.apache.beam.sdk.coders.IterableLikeCoder.encode(IterableLikeCoder.java:60)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beamWithDefault(CoderMaterializer.scala:32)
[info]   at com.spotify.scio.util.Functions$CombineFn.getAccumulatorCoder(Functions.scala:71)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:199)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:63)
[info]   at org.apache.beam.sdk.Pipeline.applyReplacement(Pipeline.java:564)
[info]   at org.apache.beam.sdk.Pipeline.replace(Pipeline.java:290)
[info]   at org.apache.beam.sdk.Pipeline.replaceAll(Pipeline.java:208)
[info]   at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:154)
[info]   at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:64)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:46)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beamWithDefault(CoderMaterializer.scala:32)
[info]   at com.spotify.scio.util.Functions$CombineFn.getAccumulatorCoder(Functions.scala:71)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:199)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:63)
[info]   at org.apache.beam.sdk.Pipeline.applyReplacement(Pipeline.java:564)
[info]   at org.apache.beam.sdk.Pipeline.replace(Pipeline.java:290)
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
[info]   at com.spotify.scio.coders.instances.SeqLikeCoder$$anonfun$encode$6.apply(ScalaCoders.scala:145)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:46)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beamWithDefault(CoderMaterializer.scala:32)
[info]   at com.spotify.scio.util.Functions$CombineFn.getAccumulatorCoder(Functions.scala:71)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:199)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:63)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:46)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beamWithDefault(CoderMaterializer.scala:32)
[info]   at com.spotify.scio.util.Functions$CombineFn.getAccumulatorCoder(Functions.scala:71)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:199)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:63)
[info]   ...
[info]   Cause: com.spotify.scio.coders.CoderException$$anon$1: - Coder was materialized at
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:49)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:52)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beam(CoderMaterializer.scala:51)
[info]   at com.spotify.scio.coders.CoderMaterializer$.beamWithDefault(CoderMaterializer.scala:32)
[info]   at com.spotify.scio.util.Functions$CombineFn.getAccumulatorCoder(Functions.scala:71)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:199)
[info]   at org.apache.beam.runners.direct.MultiStepCombine.expand(MultiStepCombine.java:63)
[info]   ...

 */
