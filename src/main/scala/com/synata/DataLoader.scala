package com.synata

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.gmail._
import com.google.api.services.gmail.model.Message
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.{BSONValue, BSONString, BSONArray, BSONDocument}
import collection.JavaConversions._
import scala.collection.immutable.Queue
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}
import reactivemongo.api._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by patwhite on 10/27/14.
 */
case class EmailAddressee (name: Option[String], emailAddress: String)

class DataLoader(accessToken: String) {

  def getGmailClient: Gmail = {
    val httpTransport = new NetHttpTransport()
    val jsonFactory = new JacksonFactory()
    val credential = new GoogleCredential()
    credential.setAccessToken(accessToken)

    new Gmail(httpTransport, jsonFactory, credential)
  }

  def getMongoCollection: BSONCollection = {
    val driver = new MongoDriver
    val connection = driver.connection(List("localhost"))

    // Gets a reference to the database "plugin"
    val db = connection("graph")

    // Gets a reference to the collection "acoll"
    // By default, you get a BSONCollection.
    db("data")

  }

  def load(): Unit = {
    val coll = getMongoCollection

    val gmail = getGmailClient
    val messageQueue = getMessages(gmail)

    val countInput = readLine(s"You have ${messageQueue.length} messages - how many should we get (Default 1000)? ")
    val count = toInt(countInput).getOrElse(1000)


    val p = Promise()

    val parallelQueue = messageQueue.toList.take(count).par
    parallelQueue.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(3))
    parallelQueue.foreach { messageId =>
      val messageReq = gmail.users().messages().get("me", messageId)
        .setFormat("metadata")
        .setMetadataHeaders(List("from", "to", "cc", "bcc", "subject"))

      //TODO(pwhite): Add a exponential back retry here
      val message = messageReq.execute()


      val props = message.getPayload.getHeaders.toList.map(m => m.getName -> m.getValue).toMap

      val mapEmail = (key: String, mapTo: String) => {
        val emailAddresses = parseEmailAddressees(props(key)).map(m => m.emailAddress.toLowerCase)
        (mapTo, BSONArray(emailAddresses.map(m => BSONString(m)).toTraversable))
      }

      val mappedProps = props.keys.map {
        case "Subject" => ("Subject", BSONString(props("Subject")))
        case "From" => mapEmail("From", "From")
        case "To" => mapEmail("To", "To")
        case "CC" => mapEmail("CC", "CC")
        case "Cc" => mapEmail("Cc", "CC")
        case "BCC" => mapEmail("BCC", "BCC")
        case "Bcc" => mapEmail("Bcc", "BCC")
        case t: String => throw new NotImplementedError(t)
      }

      val docInt = BSONDocument(mappedProps) ++ ("_id" -> messageId)
      val doc = if(docInt.get("Subject").isDefined) docInt else docInt ++ ("Subject" -> "")
      coll.save(doc)
      println(s"Saving message: $messageId - ${props.get("Subject")}")
    }


  }

  def getMessages(gmail: Gmail): Queue[String] = {
    var complete = false
    var messageQueue: Queue[String] = Queue()
    var nextPageToken: Option[String] = None

    while (!complete) {
      val req = gmail.users().messages().list("me").setMaxResults(10000L)

      if (nextPageToken.isDefined) {
        req.setPageToken(nextPageToken.get)
      }

      val resp = req.execute()
      println(s"Next Page ${resp.getNextPageToken}")

      nextPageToken = Option(resp.getNextPageToken)

      messageQueue ++= resp.getMessages.toList.map(m => m.getId)
      if (nextPageToken.isEmpty) {
        complete = true
      }
    }
    messageQueue
  }

  def toInt(s: String):Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e:Exception => None
    }
  }

  def parseEmailAddressees(str: String): Seq[EmailAddressee] = {
    // Split all the emails
    val splitStr = str.split(",")

    val seq: Seq[EmailAddressee] = splitStr.map { s =>
      tryParseEmailAddresseGmailFormat(s).recoverWith{
        case _ => tryParseEmailAddresseCanonicalFormat(s)
      }
    }.filter(i => i.isSuccess).map{ i => i.get }.toSeq

    seq
  }

  def tryParseEmailAddresseGmailFormat(str: String): Try[EmailAddressee] = {
    // Try to match with a regex that matches with the way gmail formats email fields - [firstName] [lastName] <[email]>
    val gmailFormat = """(.*)<(.*)>""".r

    val matches = gmailFormat.findAllIn(str)
    val matchData = matches.matchData

    if(matchData.hasNext) {
      val m = matchData.next

      Success(EmailAddressee(
        if(!m.group(1).isEmpty) Some(m.group(1).trim()) else None,
        m.group(2)
      ))
    } else {
      Failure(new Throwable("Email addresse is not in a gmail format."))
    }
  }

  def tryParseEmailAddresseCanonicalFormat(str: String): Try[EmailAddressee] = {
    val canonicalFormat = """([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`{|}~-]+)*@.*)""".r

    val matchData = canonicalFormat.findAllIn(str).matchData

    if(matchData.hasNext){
      val m = matchData.next

      Success(
        EmailAddressee(
          None,
          m.group(1)
        )
      )
    } else {
      Failure(new Throwable("Email addressee does not have a valid email format."))
    }
  }

}
