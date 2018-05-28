package akka.remote.transport.netty4

import java.net.{InetSocketAddress, SocketAddress}

import akka.actor.Address
import akka.event.LoggingAdapter
import akka.remote.transport.AssociationHandle
import akka.remote.transport.AssociationHandle.HandleEventListener
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.netty.NettyTransportException
import akka.util.ByteString
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}

import scala.concurrent.{Future, Promise}

//TODO 需要一个存放channel 与 listener的关系
private[remote] class TcpServerHandler(transport: NettyTransport, associationListenerFuture: Future[AssociationEventListener],
                                       val log: LoggingAdapter)
  extends ChannelInboundHandlerAdapter {
  override def channelActive(ctx: ChannelHandlerContext): Unit = {

  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {

  }

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {

  }

  private def initInbound(channel: Channel, remoteSocketAddress: SocketAddress, msg: ByteBuf): Unit = {
    associationListenerFuture.foreach {
      listener ⇒
        val remoteAddress = NettyTransport.addressFromSocketAddress(remoteSocketAddress, transport.schemeIdentifier,
          transport.system.name, hostName = None, port = None).getOrElse(
          throw new NettyTransportException(s"Unknown inbound remote address type [${remoteSocketAddress.getClass.getName}]"))
        init(channel, remoteSocketAddress, remoteAddress, msg) { listener notify InboundAssociation(_) }
    }
  }

  private def init(channel: Channel, remoteSocketAddress: SocketAddress, remoteAddress: Address, msg: ByteBuf)(
    op: (AssociationHandle ⇒ Any)): Unit = {
    import transport._
    NettyTransport.addressFromSocketAddress(channel.localAddress(), schemeIdentifier, system.name, Some(settings.Hostname), None) match {
      case Some(localAddress) ⇒
        val handle = createHandle(channel, localAddress, remoteAddress)
        handle.readHandlerPromise.future.foreach {
          listener ⇒
            registerListener(channel, listener, msg, remoteSocketAddress.asInstanceOf[InetSocketAddress])
        }
        op(handle)

      case _ ⇒ NettyTransport.gracefulClose(channel)
    }
  }

  private def createHandle(channel: Channel, localAddress: Address, remoteAddress: Address): AssociationHandle =
    new TcpAssociationHandle(localAddress, remoteAddress, transport, channel)


}


/**
  * INTERNAL API
  */
private[remote] class TcpAssociationHandle(
  val localAddress:    Address,
  val remoteAddress:   Address,
  val transport:       NettyTransport,
  private val channel: Channel)
  extends AssociationHandle {
  import transport.executionContext

  override val readHandlerPromise: Promise[HandleEventListener] = Promise()

  override def write(payload: ByteString): Boolean =
    if (channel.isWritable && channel.isOpen) {
      channel.write(Unpooled.wrappedBuffer(payload.asByteBuffer))
      true
    } else false

  override def disassociate(): Unit = NettyTransport.gracefulClose(channel)
}