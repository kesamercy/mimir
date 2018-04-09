package mimir.util

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.io.BufferedOutputStream
import org.apache.spark.SparkContext

/**
* @author ${user.name}
*/
object HadoopUtils {

  def writeToHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, localFile:File, force:Boolean = false) {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    //fs.copyFromLocalFile(false, new Path(localFile.toURI()), new Path(hdfsTargetFile))
    val hdfsPath = new Path(hdfsTargetFile)
    val exists = fs.exists(hdfsPath)
    val output = if(!exists){
      fs.create(hdfsPath)
    }
    else {
      if(force){
        fs.delete(hdfsPath, true)
        fs.create(hdfsPath)
      }
      else throw new Exception("HDFS File already exists: " + hdfsTargetFile)
    }
    val writer = new BufferedOutputStream(output)
    try {
        writer.write(Files.readAllBytes(Paths.get(localFile.getAbsolutePath))) 
    }
    finally {
        writer.close()
    }
  }
  
  def fileExistsHDFS(sparkCtx:SparkContext, hdfsTargetFile:String) : Boolean = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val hdfsPath = new Path(hdfsTargetFile)
    fs.exists(hdfsPath)
  }
  
  def getHomeDirectoryHDFS(sparkCtx:SparkContext) : String = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    fs.getHomeDirectory.toString()
  }

}