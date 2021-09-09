This directory contains AVRO files corresponding to the parquet testing files at https://github.com/apache/parquet-testing/blob/master/data/

These files were created by using spark using the commands from https://gist.github.com/Igosuki/324b011f40185269d3fc552350d21744

Roughly:
```scala
import com.github.mrpowers.spark.daria.sql.DariaWriters
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration 
import org.apache.commons.io.FilenameUtils

val fileGlobs = sc.getConf.get("spark.driver.globs")
val dest = sc.getConf.get("spark.driver.out")

val fs = FileSystem.get(new Configuration(true));
val status = fs.globStatus(new Path(fileGlobs))
for (fileStatus <- status) {
    val path = fileStatus.getPath().toString()
    try {
        val dfin = spark.read.format("parquet").load(path)
        val fileName = fileStatus.getPath().getName();
        val fileNameWithOutExt = FilenameUtils.removeExtension(fileName);
        val destination = s"${dest}/${fileNameWithOutExt}.avro"
        println(s"Converting $path to avro at $destination")
        DariaWriters.writeSingleFile(
            df = dfin,
            format = "avro",
            sc = spark.sparkContext,
            tmpFolder = s"/tmp/dw/${fileName}",
            filename = destination
        )
    } catch {
        case e: Throwable => println(s"failed to convert $path : ${e.getMessage}")
    }
}
```
