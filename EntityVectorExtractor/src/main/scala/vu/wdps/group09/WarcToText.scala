package vu.wdps.group09

import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import de.l3s.boilerpipe.extractors.CommonExtractors
import edu.stanford.nlp.ie.crf.CRFClassifier
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Level, LogManager}
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.elasticsearch.common.settings.Settings
import org.jwat.warc.WarcRecord
import vu.wdps.group09.model.FreeBaseEntity

/**
  * Created by richard on 06/12/2016.
  */
object WarcToText {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)

    // Spark setup
    val conf = new SparkConf()
      .setAppName("WarcToText")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Read from input
    val input: RDD[(LongWritable, WarcRecord)] = sc.newAPIHadoopFile(inputPath, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])

    // Get all entities grouped by the URL where they were found
    input.map(_._2)
      // Only HTTP responses
      .filter(wr => wr.header.contentTypeStr.replaceAllLiterally(" ", "") == "application/http;msgtype=response")
      // Only HTML content
      .filter(wr => Option(wr.getHttpHeader.contentType).getOrElse("").contains("text/html"))
      .filter(_.hasPayload)
      // Get payload
      .map(wr => (wr.header.getHeader("WARC-Target-URI").value, IOUtils.toString(wr.getPayload.getInputStreamComplete)))
      // Filter out German and Dutch URLs
      .filter(t => {
        val slashTokens = t._1.split('/')
        val tld = slashTokens(2).split('.').last.toLowerCase

        tld != "nl" && tld != "de" && !slashTokens.contains("nl") && !slashTokens.contains("de")
      })
      // Filter out duplicates
      .groupByKey()
      .mapValues(_.toList.head)
      // Get HTML
      .flatMap {
        case (url, payload) => {
          // Apparently it can still happen that we don't get actual XML/HTML as output, so catch exception
          try {
            Some(url, payload.substring(payload.indexOf('<')))
          } catch {
            case e: Exception => None
          }
        }
      }
      // Get relevant text from HTML
      .mapValues(CommonExtractors.ARTICLE_EXTRACTOR.getText)
      // Delete newlines and tabs from text
      .mapValues(_.replaceAllLiterally("\t", " ").replaceAllLiterally("\n", " "))
      // Map to strings
      .map(t => s"${t._1}\t${t._2}")
      // Save to file
      .saveAsTextFile(outputPath)
  }
}
