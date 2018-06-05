package akka.remote.transport.netty4

import java.security.Security

import akka.event.MarkerLoggingAdapter
import akka.remote.security.provider.AkkaProvider
import akka.remote.transport.netty.SSLSettings
import io.netty.handler.ssl.SslHandler

object SslNettySupport {
  Security addProvider AkkaProvider

  /**
    * Construct a SSLHandler which can be inserted into a Netty server/client pipeline
    */
  def apply(settings: SSLSettings, log: MarkerLoggingAdapter, isClient: Boolean): SslHandler = {
    val sslEngine = settings.getOrCreateContext(log).createSSLEngine // TODO: pass host information to enable host verification
    sslEngine.setUseClientMode(isClient)
    sslEngine.setEnabledCipherSuites(settings.SSLEnabledAlgorithms.toArray)
    sslEngine.setEnabledProtocols(Array(settings.SSLProtocol))

    if (!isClient && settings.SSLRequireMutualAuthentication) sslEngine.setNeedClientAuth(true)
    new SslHandler(sslEngine)
  }

}
