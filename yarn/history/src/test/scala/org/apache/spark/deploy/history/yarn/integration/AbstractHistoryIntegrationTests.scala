/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.history.yarn.integration

import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer

import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.rest.SpnegoUrlConnector
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider._
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.testtools.{AbstractYarnHistoryTests, FreePortFinder, HistoryServiceNotListeningToSparkContext, StubApplicationAttemptId, TimelineServiceEnabled}
import org.apache.spark.deploy.history.yarn.{YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, FsHistoryProvider, HistoryServer}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.scheduler.cluster.YarnExtensionServices
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

/**
 * Integration tests with history services setup and torn down
 */
abstract class AbstractHistoryIntegrationTests
    extends AbstractYarnHistoryTests
    with FreePortFinder
    with HistoryServiceNotListeningToSparkContext
    with TimelineServiceEnabled
    with IntegrationTestUtils {

  protected var _applicationHistoryServer: ApplicationHistoryServer = _
  protected var _timelineClient: TimelineClient = _
  protected var historyService: YarnHistoryService = _

  protected val incomplete_flag = "&showIncomplete=true"
  protected val page1_flag = "&page=1"
  protected val page1_incomplete_flag = "&page=1&showIncomplete=true"

  protected val attemptId1 = new StubApplicationAttemptId(applicationId, 1)
  protected val attemptId2 = new StubApplicationAttemptId(applicationId, 222)
  protected val attemptId3 = new StubApplicationAttemptId(applicationId, 333)

  protected val attempt1SparkId = "1"
  protected val attempt2SparkId = "2"
  protected val attempt3SparkId = "3"

  protected val no_completed_applications = "No completed applications found!"
  protected val no_incomplete_applications = "No incomplete applications found!"

  def applicationHistoryServer: ApplicationHistoryServer = {
    _applicationHistoryServer
  }

  def timelineClient: TimelineClient = {
    _timelineClient
  }

  /*
   * Setup phase creates a local ATS server and a client of it
   */

  override def setup(): Unit = {
    // abort the tests if the server is offline
    cancelIfOffline()
    super.setup()
    startTimelineClientAndAHS(sparkCtx.hadoopConfiguration)
  }

  /**
   * Set up base configuratin for integration tests, including
   * classname bindings in publisher & provider, refresh intervals and a port for the UI
   * @param sparkConf
   * @return
   */
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(OPTION_MANUAL_REFRESH_INTERVAL, "1ms")
    sparkConf.set(OPTION_BACKGROUND_REFRESH_INTERVAL, "0s")
    sparkConf.set(OPTION_DISABLE_YARN_LIVENESS_CHECKS, "true")
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
    sparkConf.set(OPTION_WINDOW_LIMIT, "0")
  }
  /**
   * Teardown stops all services, including, if set, anything in
   * <code>historyService</code>
   */
  override def teardown(): Unit = {
    describe("Teardown of history server, timeline client and history service")
    stopHistoryService(historyService)
    historyService = null
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    super.teardown()
  }

  /**
   * Stop a history service. This includes flushing its queue,
   * blocking until that queue has been flushed and closed, then
   * stopping the YARN service.
   * @param hservice history service to stop
   */
  def stopHistoryService(hservice: YarnHistoryService): Unit = {
    if (hservice != null && hservice.serviceState == YarnHistoryService.StartedState) {
      flushHistoryServiceToSuccess()
      hservice.stop()
      awaitServiceThreadStopped(hservice, TEST_STARTUP_DELAY, false)
    }
  }

  /**
   * Create a SPNEGO-enabled URL Connector
   * @return a URL connector for issuing HTTP requests
   */
  protected def createUrlConnector(): SpnegoUrlConnector = {
    SpnegoUrlConnector.newInstance(sparkCtx.hadoopConfiguration,
      new DelegationTokenAuthenticatedURL.Token)
  }

  /**
   * Create the client and the app server
   * @param conf the hadoop configuration
   */
  protected def startTimelineClientAndAHS(conf: Configuration): Unit = {
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    _timelineClient = TimelineClient.createTimelineClient()
    _timelineClient.init(conf)
    _timelineClient.start()
    _applicationHistoryServer = new ApplicationHistoryServer()
    _applicationHistoryServer.init(_timelineClient.getConfig)
    _applicationHistoryServer.start()
    // Wait for AHS to come up
    val endpoint = YarnTimelineUtils.timelineWebappUri(conf, "")
    awaitURL(endpoint.toURL, TEST_STARTUP_DELAY)
  }

  protected def createTimelineQueryClient(): TimelineQueryClient = {
    new TimelineQueryClient(historyService.timelineWebappAddress,
      sparkCtx.hadoopConfiguration, createClientConfig())
  }

  /**
   * Put a timeline entity to the timeline client; this is expected
   * to eventually make it to the history server
   * @param entity entity to put
   * @return the response
   */
  def putTimelineEntity(entity: TimelineEntity): TimelinePutResponse = {
    assertNotNull(_timelineClient, "timelineClient")
    _timelineClient.putEntities(entity)
  }

  /**
   * Marshall and post a spark event to the timeline; return the outcome
   * @param sparkEvt event
   * @param time event time
   * @return a triple of the wrapped event, marshalled entity and the response
   */
  protected def postEvent(sparkEvt: SparkListenerEvent, time: Long):
      (TimelineEvent, TimelineEntity, TimelinePutResponse) = {
    val event = toTimelineEvent(sparkEvt, time).get
    val entity = newEntity(time)
    entity.addEvent(event)
    val response = putTimelineEntity(entity)
    val description = describePutResponse(response)
    logInfo(s"response: $description")
    assert(response.getErrors.isEmpty, s"errors in response: $description")
    (event, entity, response)
  }

  /**
   * flush the history service of its queue, await it to complete,
   * then assert that there were no failures
   */
  protected def flushHistoryServiceToSuccess(): Unit = {
    flushHistoryServiceToSuccess(historyService)
  }

  /**
   * Flush a history service to success
   * @param history service to flush
   * @param delay time to wait for an empty queue
   */
  def flushHistoryServiceToSuccess(history: YarnHistoryService,
      delay: Int = TEST_STARTUP_DELAY): Unit = {
    assertNotNull(history, "null history queue")
    historyService.asyncFlush()
    awaitEmptyQueue(history, delay)
    assert(0 === history.postFailures, s"Post failure count: $history")
  }

  /**
   * Create a history provider instance, following the same process
   * as the history web UI itself: querying the configuration for the
   * provider and falling back to the [[FsHistoryProvider]]. If
   * that falback does take place, however, and assertion is raised.
   * @param conf configuration
   * @return the instance
   */
  protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val providerName = conf.getOption("spark.history.provider")
        .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Class.forName(providerName)
        .getConstructor(classOf[SparkConf])
        .newInstance(conf)
        .asInstanceOf[ApplicationHistoryProvider]
    assert(provider.isInstanceOf[YarnHistoryProvider],
            s"Instantiated $providerName to get $provider")

    provider.asInstanceOf[YarnHistoryProvider]
  }

  /**
   * Crete a history server and maching provider, execute the
   * probe against it. After the probe completes, the history server
   * is stopped.
   * @param probe probe to run
   */
  def webUITest(name: String, probe: (URL, YarnHistoryProvider) => Unit): Unit = {
    val (_, server, webUI, provider) = createHistoryServer(findPort())
    try {
      server.bind()
      describe(name)
      probe(webUI, provider)
    } finally {
      describe("stopping history service")
      try {
        server.stop()
      } catch {
        case e: Exception =>
          logInfo(s"In History Server teardown: $e", e)
      }
    }
  }

  /**
   * Probe the empty web UI for not having any completed apps; expect
   * a text/html response with specific text and history provider configuration
   * elements.
   * @param webUI web UI
   * @param provider provider
   */
  def probeEmptyWebUI(webUI: URL, provider: YarnHistoryProvider): String = {
    val body: String = getHtmlPage(webUI,
       "<title>History Server</title>"
        :: no_completed_applications
        :: YarnHistoryProvider.KEY_PROVIDER_NAME
        :: YarnHistoryProvider.KEY_PROVIDER_DETAILS
        :: Nil)
    logInfo(s"$body")
    body
  }

  /**
   * Get an HTML page. Includes a check that the content type is `text/html`
   * @param page web UI
   * @param checks list of strings to assert existing in the response
   * @return the body of the response
   */
  protected def getHtmlPage(page: URL, checks: List[String]): String = {
    val outcome = createUrlConnector().execHttpOperation("GET", page, null, "")
    logDebug(s"$page => $outcome")
    assert(outcome.contentType.startsWith("text/html"), s"content type of $outcome")
    val body = outcome.responseBody
    assertStringsInBody(body, checks)
    body
  }

  /**
   * Assert that a list of checks are in the HTML body
   * @param body body of HTML (or other string)
   * @param checks list of strings to assert are present
   */
  def assertStringsInBody(body: String, checks: List[String]): Unit = {
    var missing: List[String] = Nil
    var text = "[ "
    checks foreach { check =>
      if (!body.contains(check)) {
        missing = check :: missing
        text = text +"\"" + check +"\" "
      }
    }
    text = text +"]"
    if (missing.nonEmpty) {
      fail(s"Did not find $text in\n$body")
    }
  }

  /**
   * Create a [[HistoryServer]] instance with a coupled history provider.
   * @param defaultPort a port to use if the property `spark.history.ui.port` isn't
   *          set in the spark context. (default: 18080)
   * @return (port, server, web UI URL, history provider)
   */
  protected def createHistoryServer(defaultPort: Int = 18080):
     (Int, HistoryServer, URL, YarnHistoryProvider) = {
    val conf = sparkCtx.getConf
    val securityManager = new SecurityManager(conf)
    val args: List[String] = Nil
    val port = conf.getInt("spark.history.ui.port", defaultPort)
    val provider = createHistoryProvider(sparkCtx.getConf)
    val server = new HistoryServer(conf, provider, securityManager, port)
    val webUI = new URL("http", "localhost", port, "/")
    (port, server, webUI, provider)
  }


  /**
   * Get the provider UI with an assertion failure if none came back
   * @param provider provider
   * @param appId app ID
   * @param attemptId optional attempt ID
   * @return the provider UI retrieved
   */
  def getAppUI(provider: YarnHistoryProvider,
      appId: String,
      attemptId: Option[String]): SparkUI = {
    val ui = provider.getAppUI(appId, attemptId)
    assertSome(ui, s"Failed to retrieve App UI under ID $appId attempt $attemptId")
    ui.get
  }


  /**
   * closing context generates an application stop
   */
  def stopContextAndFlushHistoryService(): Unit = {
    describe("stopping context")
    sparkCtx.stop()
    flushHistoryServiceToSuccess()
  }

  /**
   * Create and queue a new [HandleSparkEvent] from the data
   * @param sparkEvent spark event
   */
  def enqueue(sparkEvent: SparkListenerEvent): Unit = {
    eventTime(sparkEvent).getOrElse {
      throw new RuntimeException(s"No time from $sparkEvent")
    }
    assert(historyService.enqueue(sparkEvent))
  }

  /**
   * Post up multiple attempts with the second one a success
   */
  def postMultipleAttempts(): Unit = {
    logDebug("posting app start")
    val startTime = 10000
    historyService = startHistoryService(sparkCtx, applicationId, Some(attemptId1))
    val start1 = appStartEvent(startTime,
      sparkCtx.applicationId,
      Utils.getCurrentUserName(),
      Some(attempt1SparkId))
    enqueue(start1)
    enqueue(jobStartEvent(10001, 1))
    enqueue(jobFailureEvent(10002, 1, new scala.RuntimeException("failed")))
    enqueue(appStopEvent(10003))
    flushHistoryServiceToSuccess()
    stopHistoryService(historyService)

    // second attempt
    val start2Time = 20000
    historyService = startHistoryService(sparkCtx, applicationId, Some(attemptId2))
    val start2 = appStartEvent(start2Time,
      sparkCtx.applicationId,
      Utils.getCurrentUserName(),
      Some(attempt2SparkId))
    enqueue(start2)

    enqueue(jobStartEvent(20000, 1))
    enqueue(jobSuccessEvent(20002, 1))
    enqueue(appStopEvent(20003))

    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
  }


  /**
   * Get the spark UI or fail
   * @param provider provider
   * @param appId app id
   * @param attemptIdOption optional attempt ID
   * @return the UI
   */
  def fetchSparkUI(provider: YarnHistoryProvider, appId: String,
      attemptIdOption: Option[String]): SparkUI = {
    val appUIwrapper = provider.getAppUI(appId, attemptIdOption)
    val ui = appUIwrapper match {
      case Some(yarnAppUI) =>
        yarnAppUI
      // success
      case None => fail(s"Did not get a UI for $appId/$attemptIdOption")
    }
    ui
  }

}
