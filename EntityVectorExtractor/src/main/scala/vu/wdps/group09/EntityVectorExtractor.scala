package vu.wdps.group09

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri, MultiSearchResult}
import de.l3s.boilerpipe.extractors.CommonExtractors
import edu.stanford.nlp.ie.crf.CRFClassifier
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.settings.Settings
import org.jwat.warc.WarcRecord
import vu.wdps.group09.model.FreeBaseEntity

import scala.collection.JavaConversions._

class EntityVectorExtractor {
  object LogHolder extends Serializable {
    @transient lazy val log: Logger = LogManager.getRootLogger
  }

  def main(args: Array[String]) = {
    val inputPath = args(0)
    val outputPath = args(0)

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)

    // Spark setup
    val conf = new SparkConf()
      .setAppName("EntityVectorExtractor")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Read from input
    val input: RDD[(LongWritable, WarcRecord)] = sc.newAPIHadoopFile(inputPath, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])

    // Get all entities grouped by the URL where they were found
    val entitiesByURL: RDD[(String, List[FreeBaseEntity])] = input.map(_._2)
      // Only HTTP responses
      .filter(wr => wr.header.contentTypeStr == "application/http; msgtype=response")
      // Only HTML content
      .filter(wr => Option(wr.getHttpHeader.contentType).getOrElse("").contains("text/html"))
      .filter(_.hasPayload)
      // Get payload
      .map(wr => (wr.header.getHeader("WARC-Target-URI").value, IOUtils.toString(wr.getPayload.getInputStreamComplete)))
      // Get HTML
      .flatMap {
        case (url, payload) => {
          // Apparently it can still happen that we don't get actual XML/HTML as output, so catch exception
          try {
            Option(url, payload.substring(payload.indexOf('<')))
          } catch {
            case e: Exception => None
          }
        }
      }
      // Get relevant text from HTML
      .mapValues(CommonExtractors.ARTICLE_EXTRACTOR.getText)
      // Extract named entities from text
      .mapPartitions(iter => {
        // Load classifier
        val classifier = CRFClassifier.getClassifierNoExceptions("edu/stanford/nlp/models/ner/english.conll.4class.caseless.distsim.crf.ser.gz")

        iter.map {
          case (url, text) =>
            val triples = classifier.classifyToCharacterOffsets(text)
            val surfaceForms = triples.map(t => text.substring(t.second, t.third))
            (url, surfaceForms)
        }
      })
      // Split entities in groups of 50 to avoid too large Elastic Search requests
      .flatMapValues(xs => split[String](xs.toList, 50))
      // Do an ElasticSearch request for each entity
      .mapPartitions(iter => {
        val settings = Settings.builder().put("cluster.name", "web-data-processing-systems").build()
        val client = ElasticClient.transport(settings, ElasticsearchClientUri("10.149.0.127:9300"))

        iter.map(t => {
          if (t._2.nonEmpty) {
            // Run a multi search query for all entities in this document
            val surfaceForms = t._2.map(_.toLowerCase)
              .map(_.replaceAll("&", " ").replaceAll("\"", " ").replaceAll("\n", " ").replace("\r", ""))
              .map(QueryParserUtil.escape)
            val multisearchResponse = elasticSearch(client, surfaceForms, 5)

            if (multisearchResponse.nonEmpty) {
              // Map responses to entity labels
              val entities = multisearchResponse.get.getResponses()
                .flatMap(r => {
                  if (!r.isFailure && r.getResponse.hits.nonEmpty) {
                    val hit = r.getResponse.hits(0)
                    Some(FreeBaseEntity(hit.sourceAsMap("resource").toString.replace("fbase:m.", "/"), hit.sourceAsMap("label").toString))
                  }
                  else
                    None
                }).toList
              (t._1, entities)
            } else {
              (t._1, List())
            }
          } else {
            (t._1, List())
          }
        })
      })
      .cache()

    // Get a distinct set of entities zipped with indexes (which will serve as the new IDs)
    val entityIndexTuples: RDD[(FreeBaseEntity, Long)] = entitiesByURL
      .values
      .flatMap(i => i)
      .distinct
      .zipWithIndex
      .cache()

    // Get a set of (id, index) tuples which will form the basis of our freebase id -> index map
    val idIndexTuples:RDD[(String, Long)] = entityIndexTuples
      .map(t => (t._1.id, t._2))

    // Get a set of (index, label) tuples which will form the basis of our index -> label map
    val indexLabelTuples: RDD[(Long, String)] = entityIndexTuples
      .map(t => (t._2, t._1))
      .mapValues(_.label)

    // Maps freebase IDs to indexes (which function as new IDs)
    val indexByIdMap = sc.broadcast(idIndexTuples.collectAsMap())

    // Get entity vectors for each document and write to file
    entitiesByURL
      // Map entity to index in vector
      .mapValues(_.flatMap(e => indexByIdMap.value.get(e.id)))
      // Count occurrence of each entity (or index)
      .mapValues(_
        .map(i => (i, 1))
        .groupBy(_._1)
        .mapValues(_.length)
      )
      // Map (index, count) tuples to strings
      .mapValues(_.map(t => s"${t._1}x${t._2}"))
      // Map (url, vector) tuples to strings
      .map(t => s"${t._1}\t${t._2.mkString(",")}")
      // Write to file
      .saveAsTextFile(outputPath + "_documentEntityVectors")

    // Write label map to file
    indexLabelTuples
      .sortByKey(true)
      .map(_._2)
      .saveAsTextFile(outputPath + "_entityLabelMap")
  }

  /**
    * Splits a list in multiple lists of a fixed size.
    *
    * @param xs The list to split
    * @param n  The (maximum) size of the produced lists
    * @tparam T The type of the elements in the list
    * @return
    */
  def split[T](xs: List[T], n: Integer): List[List[T]] = {
    if (xs.isEmpty)
      Nil
    else
      (xs take n) :: split(xs drop n, n)
  }

  /**
    * Do an elastic search using a given client and surface forms to query. When the elastic search times out it will
    * retry, up till a given maximum number of attempts.
    *
    * @param client       The client to use for the query
    * @param surfaceForms The surface forms to query
    * @param attempts     The maximum number of attempts
    * @return
    */
  def elasticSearch(client: ElasticClient, surfaceForms: Iterable[String], attempts: Integer): Option[MultiSearchResult] = {
    try {
      val multisearchResponse = client.execute(
        multi(
          surfaceForms.map(s => search("freebase" / "label") query s limit 1)
        )
      ).await

      Some(multisearchResponse)
    } catch {
      case e: java.util.concurrent.TimeoutException =>
        if (attempts > 0)
          elasticSearch(client, surfaceForms, attempts - 1)
        else {
          LogHolder.log.warn("Elastic Search Timeout")
          None
        }
    }
  }
}
