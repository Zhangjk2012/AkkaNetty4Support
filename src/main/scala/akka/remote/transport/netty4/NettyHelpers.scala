package akka.remote.transport.netty4

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}


private[netty] trait NettyHelpers {
  protected def channelActive(ctx: ChannelHandlerContext): Unit = ()
}


private[netty] trait NettyServerHelpers extends ChannelInboundHandlerAdapter with NettyHelpers{
  override def channelRegistered(ctx: ChannelHandlerContext): Unit = super.channelRegistered(ctx)

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = super.channelUnregistered(ctx)

  override def channelActive(ctx: ChannelHandlerContext): Unit = super.channelActive(ctx)

  override def channelInactive(ctx: ChannelHandlerContext): Unit = super.channelInactive(ctx)

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = super.channelRead(ctx, msg)

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = super.channelReadComplete(ctx)

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit = super.userEventTriggered(ctx, evt)

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = super.exceptionCaught(ctx, cause)
}

private[netty] trait NettyClientHelpers extends ChannelInboundHandlerAdapter with NettyHelpers {

}