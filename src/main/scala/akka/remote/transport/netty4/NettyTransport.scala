package akka.remote.transport.netty4

import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CancellationException, ThreadFactory}

import akka.actor.{Address, ExtendedActorSystem}
import akka.event.Logging
import akka.remote.RARP
import akka.remote.transport.AssociationHandle.HandleEventListener
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.netty.NettyTransportSettings.{Tcp, Udp}
import akka.remote.transport.netty.{NettyTransportException, NettyTransportSettings}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.Helpers
import com.typesafe.config.Config
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.group.{ChannelGroup, ChannelGroupFuture, ChannelGroupFutureListener, DefaultChannelGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelHandlerContext, _}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.util.concurrent.GlobalEventExecutor

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.Try
import scala.util.control.NonFatal

private[transport] object NettyTransport {
  // 4 bytes will be used to represent the frame length. Used by netty LengthFieldPrepender downstream handler.
  val FrameLengthFieldLength = 4

  val OS_NAME = System.getProperty("os.name", "")

  val isUseEpoll = OS_NAME match {
    case "" => false
    case _ if (Helpers.toRootLowerCase(OS_NAME).indexOf("linux") >= 0 && Epoll.isAvailable())  => true
  }

  def gracefulClose(channel: Channel)(implicit ec: ExecutionContext): Unit = {
    def always(c: ChannelFuture) = channelFuture2ScalaFuture(c) recover { case _ ⇒ c.channel() }
    for {
      _ ← always { channel.write(Unpooled.buffer(0)) } // Force flush by waiting on a final dummy write
      _ ← always { channel.disconnect() }
    } channel.close()
  }

  val uniqueIdCounter = new AtomicInteger(0)

  def addressFromSocketAddress(address: SocketAddress, schemeIdentifier: String, systemName: String,
                               hostName: Option[String], port: Option[Int]): Option[Address] = address match {
    case sa: InetSocketAddress ⇒ Some(Address(schemeIdentifier, systemName,
      hostName.getOrElse(sa.getHostString), port.getOrElse(sa.getPort)))
    case _ ⇒ None
  }

  // Need to do like this for binary compatibility reasons
  def addressFromSocketAddress(address: SocketAddress, schemeIdentifier: String, systemName: String,
                               hostName: Option[String]): Option[Address] =
    addressFromSocketAddress(address, schemeIdentifier, systemName, hostName, port = None)

  def channelFuture2ScalaFuture(channelFuture: ChannelFuture): Future[Channel] = {
    val p = Promise[Channel]()
    channelFuture.addListener(new ChannelFutureListener(){
      override def operationComplete(future: ChannelFuture): Unit = p complete Try {
        if (future.isSuccess) future.channel()
        else if (future.isCancelled) throw new CancellationException
        else throw future.cause()
      }
    })
    p.future
  }

  def channelGroupFuture2ScalaFuture(channelFuture: ChannelGroupFuture) = {
    val p = Promise[ChannelGroup]()
    import scala.collection.JavaConverters._
    channelFuture.addListener(new ChannelGroupFutureListener(){
      override def operationComplete(future: ChannelGroupFuture): Unit = p complete Try {
        if (future.isSuccess) future.group
        else throw future.iterator.asScala.collectFirst {
          case f if f.isCancelled ⇒ new CancellationException
          case f if !f.isSuccess  ⇒ f.cause()
        } getOrElse new IllegalStateException("Error reported in ChannelGroupFuture, but no error found in individual futures.")
      }
    })
    p.future
  }

  /**
    * create event loop group
    * @param threadNum      thread numbers
    * @param threadFactory  thread factory
    * @return
    */
  def createEventLoopGroup(threadNum: Int, threadFactory: ThreadFactory): EventLoopGroup = {
    val eventLoopGroup = isUseEpoll match {
      case true =>
        new EpollEventLoopGroup(threadNum, threadFactory)
      case false => new NioEventLoopGroup(threadNum, threadFactory)
    }
    eventLoopGroup
  }

  /**
    * create a named thread factory.
    * @param threadNum
    * @param threadName
    * @param daemon
    * @return
    */
  def namedThreadFactory(threadNum: Int, threadName: String, daemon: Boolean): ThreadFactory = {
    val threadFactory = new ThreadFactory {
      import java.util.concurrent.atomic.AtomicInteger
      val threadIndex = new AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, s"${threadName}_${threadNum}_${threadIndex.getAndIncrement()}")
        thread.setDaemon(daemon)
        thread
      }
    }
    threadFactory
  }

}

class Netty4TransportSettings(config: Config) extends NettyTransportSettings(config){
  import config._
  val ServerSocketThreadName = getString("server-socket-pool-name")
  val ClientSocketThreadName = getString("client-socket-pool-name")
}

class NettyTransport (val settings: Netty4TransportSettings, val system: ExtendedActorSystem) extends Transport{

  def this(system: ExtendedActorSystem, conf: Config) = this(new Netty4TransportSettings(conf), system)

  import akka.remote.transport.netty4.NettyTransport._
  import settings._

  implicit val executionContext: ExecutionContext =
    settings.UseDispatcherForIo.orElse(RARP(system).provider.remoteSettings.Dispatcher match {
      case ""             ⇒ None
      case dispatcherName ⇒ Some(dispatcherName)
    }).map(system.dispatchers.lookup).getOrElse(system.dispatcher)

  val bossEventLoopGroup = createEventLoopGroup(1,
        namedThreadFactory(1, s"${ServerSocketThreadName}_Boss", true))
  val workEventLoopGroup = createEventLoopGroup(ServerSocketWorkerPoolSize,
        namedThreadFactory(ServerSocketWorkerPoolSize, s"${ServerSocketThreadName}_Worker", false))

  /** 存放所有Server端连接的channel */
  val channelGroup = new DefaultChannelGroup("akka-netty-transport-driver-channelgroup-" +
    uniqueIdCounter.getAndIncrement, GlobalEventExecutor.INSTANCE)

  @volatile private var serverChannel: Channel = _
  @volatile private var localAddress: Address = _
  @volatile private var boundTo: Address = _


  private val associationListenerPromise: Promise[AssociationEventListener] = Promise()

  val inboundBootstrap = TransportMode match {
    case Tcp => {
      import java.lang.{Boolean => JBoolean}
      val b = new ServerBootstrap()
      b.group(bossEventLoopGroup, workEventLoopGroup)
      isUseEpoll match {
        case true  => b.channel(classOf[EpollServerSocketChannel])
        case false => b.channel(classOf[NioServerSocketChannel])
      }
      b.option[Integer](ChannelOption.SO_BACKLOG, Backlog)
        .option[JBoolean](ChannelOption.SO_REUSEADDR, TcpReuseAddr)
        .option[JBoolean](ChannelOption.SO_KEEPALIVE, TcpKeepalive)
        .childOption[JBoolean](ChannelOption.TCP_NODELAY, TcpNodelay)
      SendBufferSize.foreach(size => b.childOption[Integer](ChannelOption.SO_SNDBUF, size))
      ReceiveBufferSize.foreach(size => b.childOption[Integer](ChannelOption.SO_RCVBUF, size))
      WriteBufferHighWaterMark.flatMap(high => WriteBufferLowWaterMark.map(low => (low, high))).foreach(o => {
        b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(o._1, o._2))
      })
      b.childHandler(new ChannelInitializer[SocketChannel](){
        override def initChannel(ch: SocketChannel): Unit = {
          val pipeline = ch.pipeline()
          pipeline.addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(
            maximumPayloadBytes,
            0,
            FrameLengthFieldLength,
            0,
            FrameLengthFieldLength, // Strip the header
            true
          ))
          pipeline.addLast("FrameEncoder", new LengthFieldPrepender(FrameLengthFieldLength))
          // TODO: need add a ssl context and tcp handler

          val handler = new TcpServerHandler(NettyTransport.this, associationListenerPromise.future, log)
          pipeline.addLast("ServerHandler", handler)
        }
      })
      b
    }
    case Udp => {

    }
  }


  private val log = Logging.withMarker(system, this.getClass)

  override def schemeIdentifier: String = (if (EnableSsl) "ssl." else "") + TransportMode

  // fixme Add configurable subnet filtering
  override def isResponsibleFor(address: Address): Boolean = true

  override def maximumPayloadBytes: Int = MaxFrameSize

  // TODO: This should be factored out to an async (or thread-isolated) name lookup service #2960
  def addressToSocketAddress(address: Address): Future[InetSocketAddress] = address match {
    case Address(_, _, Some(host), Some(port)) ⇒ Future { blocking { new InetSocketAddress(InetAddress.getByName(host), port) } }
    case _                                     ⇒ Future.failed(new IllegalArgumentException(s"Address [$address] does not contain host or port information."))
  }

  // 配置监听事件
  override def listen: Future[(Address, Promise[Transport.AssociationEventListener])] = {
    for {
      // 这里bind的是bind-port与bind-hostname，如果没有配置，则，使用port与hostname。
      address ← addressToSocketAddress(Address("", "", settings.BindHostname, settings.BindPortSelector))
    } yield {
      try {
        val newServerChannel = inboundBootstrap match {
          case b: ServerBootstrap         ⇒ b.bind(address).channel()
          //case b: ConnectionlessBootstrap ⇒ b.bind(address)
        }

        // Block reads until a handler actor is registered
        newServerChannel.config().setAutoRead(false);

        // fixme 这里需要把自己也添加进去？
        channelGroup.add(newServerChannel)

        serverChannel = newServerChannel

        addressFromSocketAddress(newServerChannel.localAddress(), schemeIdentifier, system.name, Some(settings.Hostname),
          if (settings.PortSelector == 0) None else Some(settings.PortSelector)) match {
          case Some(address) ⇒
            addressFromSocketAddress(newServerChannel.localAddress(), schemeIdentifier, system.name, None, None) match {
              case Some(address) ⇒ boundTo = address
              case None          ⇒ throw new NettyTransportException(s"Unknown local address type [${newServerChannel.getLocalAddress.getClass.getName}]")
            }
            localAddress = address
            associationListenerPromise.future.foreach { _ ⇒
              newServerChannel.config.setAutoRead(true)
              //fixme 是否需要手动触发读取事件，需要测试
              // newServerChannel.read()
            }
            (address, associationListenerPromise)
          case None ⇒ throw new NettyTransportException(s"Unknown local address type [${newServerChannel.localAddress().getClass.getName}]")
        }
      } catch {
        case NonFatal(e) ⇒ {
          log.error("failed to bind to {}, shutting down Netty transport", address)
          try { shutdown() } catch { case NonFatal(e) ⇒ } // ignore possible exception during shutdown
          throw e
        }
      }
    }
  }

  override def associate(remoteAddress: Address): Future[AssociationHandle] = ???

  override def shutdown(): Future[Boolean] = ???
}