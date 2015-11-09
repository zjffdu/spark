package org.apache.spark.sql.execution.datasources.remote

import java.io.{InputStreamReader, BufferedReader}
import java.security.cert.CertificateException
import javax.net.ssl._
import java.security.cert.X509Certificate
import org.apache.commons.io.input.ReaderInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils


object RestUtils {
  /*
   *  fix for
   *    Exception in thread "main" javax.net.ssl.SSLHandshakeException:
   *       sun.security.validator.ValidatorException:
   *           PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException:
   *               unable to find valid certification path to requested target
   */
  private val trustAllCerts: Array[TrustManager] = Array[TrustManager](new X509TrustManager() {
    override def getAcceptedIssuers: Array[X509Certificate] = null
    override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
    override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
  })

  val sc: SSLContext = SSLContext.getInstance("SSL")
  sc.init(null, trustAllCerts, new java.security.SecureRandom())
  HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory)
  // Create all-trusting host name verifier
  val allHostsValid: HostnameVerifier = new HostnameVerifier() {
    def verify(hostname: String, session: SSLSession): Boolean = {
      return true
    }
  }
  // Install the all-trusting host verifier
  HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid)

  /*
   * end of the fix
   */

  def download(urls: Seq[String], destFolder: String, fs: FileSystem): Unit = {

    val output = fs.create(new Path(destFolder + "/a.txt"))
    import scala.io.Source
    urls.foreach(url => {
      IOUtils.copyBytes(new ReaderInputStream(Source.fromURL(url).reader()), output, fs.getConf)
      output.close()
    })
  }

  //  def get(url: String) = {
  //    val client = HttpClientBuilder.create().build();
  //    val request = new HttpGet(url);
  //    val response = client.execute(request);
  //    scala.io.Source.fromInputStream(response.getEntity().getContent()).getLines.mkString
  //  }
  //
  //  def main(args:Array[String]): Unit = {
  //    println(get("http://baidu.com"))
  //  }


}
