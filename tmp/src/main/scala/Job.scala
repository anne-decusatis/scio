import com.spotify.scio.avro._
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import scala.collection.JavaConverters._
import com.spotify.scio.options.ScioOptions

object Job {

  val schemaR = TestRecord.SCHEMA$

  //scalastyle:off method.length cyclomatic.complexity
  def main(argv: Array[String]) = {
    val (sc, args) = ContextAndArgs(argv)
    val (opts, _) = ScioContext.parseArguments[ScioOptions](argv)

    val (input, output) = (args("input"), args("output"))

    FileSystems.setDefaultPipelineOptions(opts)

    val coll = sc.avroFile[GenericRecord](input, schemaR)

    val sampledCollection = coll
      .map(record => (record, record.get("string_field")))
      .map(_._1)

    val r = sampledCollection.saveAsAvroFile(output, schema = schemaR)
    sc.close().waitUntilDone()
    r
  }
  //scalastyle:on method.length cyclomatic.complexity

}
