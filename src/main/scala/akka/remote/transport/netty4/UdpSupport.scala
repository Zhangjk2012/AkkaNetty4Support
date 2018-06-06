package akka.remote.transport.netty4

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.Address
import akka.event.LoggingAdapter
import akka.remote.transport.AssociationHandle
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.util.ByteString
import io.netty.buffer.Unpooled
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.{Channel, ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.{Future, Promise}

class UdpServerClient(_transport: NettyTransport, future: Future[AssociationEventListener], val log: LoggingAdapter)
  extends SimpleChannelInboundHandler[DatagramPacket] {

  private[this] var listener: HandleEventListener = _

  override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket): Unit = {
    if (log.isDebugEnabled) {
      log.debug("Receive a msg, from [{}]", msg.sender())
    }
    val byteBuf = msg.content()
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

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    log.info("Udp Server Channel active.initialize the ProtocolStateActor.")
    ctx.channel().config().setAutoRead(false)
    future.foreach(listener => {
      val udpAssociationHandle = new UdpAssociationHandle(null, null, ctx.channel(), _transport)
      udpAssociationHandle.readHandlerPromise.future.foreach(
        listener => {
          ctx.channel().config().setAutoRead(true)
          this.listener = listener
        }
      )
      listener notify InboundAssociation(udpAssociationHandle)
    })
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    log.info("Udp Server channel inactive. stop the ProtocolStateActor")
    listener notify Disassociated(AssociationHandle.Unknown)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.warning("Udp Server channel has a exception.", cause)
    ctx.channel().close()
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

}