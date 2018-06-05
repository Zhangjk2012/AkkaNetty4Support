package akka.remote.transport.netty4

import java.net.SocketAddress

import akka.actor.Address
import akka.event.LoggingAdapter
import akka.remote.transport.AssociationHandle
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.netty.NettyTransportException
import akka.util.ByteString
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}

import scala.concurrent.{Future, Promise}

private[remote] class TcpServerHandler(transport: NettyTransport, associationListener: Future[AssociationEventListener],
                                       val log: LoggingAdapter) extends ChannelInboundHandlerAdapter {

  private[this] var remoteAddress = _
  private[this] var remoteSocketAddress: SocketAddress = _
  private[this] var listener: HandleEventListener = _

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    // 当有连接进来，需要注册一个Actor，用来处理消息，这里是:ProtocolStateActor
    // associationListener里面持有的Listener是 AkkaProtocolManager
    initInbound(ctx.channel())
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    // 连接断开
    log.info("Remote [{}] disconnection.", remoteSocketAddress)
    // 不需要关闭listener中的ProtocolStateActor， 因为发送Disassociated, actor会自己关闭
    listener.notify(Disassociated(AssociationHandle.Unknown))
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    if (msg.isInstanceOf[ByteBuf]) {
      val byteBuf = msg.asInstanceOf[ByteBuf]
      if (byteBuf.hasArray) {
        val array = byteBuf.array
        val offset = byteBuf.arrayOffset + byteBuf.readerIndex
        val length = byteBuf.readableBytes
        val byteString = ByteString.fromArrayUnsafe(array, offset, length)
        listener notify InboundPayload(byteString)
      } else {
        val length = byteBuf.readableBytes
        val array = new Array[Byte](length)
        byteBuf.getBytes(byteBuf.readerIndex(), array)
        val byteString = ByteString(array)
        listener notify InboundPayload(byteString)
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.warning("Remote connection to [{}] failed with {}", remoteSocketAddress, cause)
    ctx.close()
  }

  private def initInbound(channel: Channel): Unit = {
    remoteSocketAddress = channel.remoteAddress()
    associationListener.foreach {
      // 发送InboundAssociation，得到ProtocolStateActor
      listener ⇒
        remoteAddress = NettyTransport.addressFromSocketAddress(remoteSocketAddress, transport.schemeIdentifier,
          transport.system.name, hostName = None, port = None).getOrElse(
          throw new NettyTransportException(s"Unknown inbound remote address type" +
            s" [${remoteSocketAddress.getClass.getName}]"))
        init(channel, remoteSocketAddress, remoteAddress) {
          listener notify InboundAssociation(_)
        }
    }
  }

  private def init(channel: Channel, remoteSocketAddress: SocketAddress, remoteAddress: Address)(
    op: (AssociationHandle ⇒ Any)): Unit = {
    import transport._
    NettyTransport.addressFromSocketAddress(channel.localAddress(), schemeIdentifier, system.name,
      Some(settings.Hostname), None) match {
      case Some(localAddress) ⇒
        val handle = createHandle(channel, localAddress, remoteAddress)
        handle.readHandlerPromise.future.foreach {
          listener ⇒
            // 存放返回的listener
            this.listener = listener
            channel.config().setAutoRead(true)
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
                                            val localAddress: Address,
                                            val remoteAddress: Address,
                                            val transport: NettyTransport,
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