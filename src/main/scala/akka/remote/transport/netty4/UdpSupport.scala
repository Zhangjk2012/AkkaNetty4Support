package akka.remote.transport.netty4

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.Address
import akka.event.LoggingAdapter
import akka.remote.transport.AssociationHandle
import akka.remote.transport.AssociationHandle.HandleEventListener
import akka.remote.transport.Transport.AssociationEventListener
import akka.util.ByteString
import io.netty.buffer.Unpooled
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.{Channel, ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.{Future, Promise}

class UdpServerClient(_transport: NettyTransport, future: Future[AssociationEventListener], val log: LoggingAdapter)
  extends SimpleChannelInboundHandler[DatagramPacket] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket): Unit = {

  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {

  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {

  }
}


/**
  * INTERNAL API
  */
private[remote] class UdpAssociationHandle(
                                            val localAddress:      Address,
                                            val remoteAddress:     Address,
                                            private val channel:   Channel,
                                            private val transport: NettyTransport) extends AssociationHandle {

  override val readHandlerPromise: Promise[HandleEventListener] = Promise()

  override def write(payload: ByteString): Boolean = {
    if (!channel.isOpen)
      channel.connect(new InetSocketAddress(InetAddress.getByName(remoteAddress.host.get), remoteAddress.port.get))

    if (channel.isWritable && channel.isOpen) {
      channel.write(Unpooled.wrappedBuffer(payload.asByteBuffer))
      true
    } else false
  }

  override def disassociate(): Unit = try channel.close()
  finally transport.udpConnectionTable.remove(transport.addressToSocketAddress(remoteAddress))

}