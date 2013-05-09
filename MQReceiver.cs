/*********************************************************************
 * 名称： MQReceiver.cs
 * 功能： 消息接收者
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.21
 * 最新： 
 * 版本： 1.1.0
 * 修订：
 *        1.0.0 --- 从指定的消息队列同步/异步接收消息
 *        1.1.0 --- 廖洪周重构代码，By 2009-04-11
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// 消息接收者
    /// </summary>
    public class MQReceiver : MQConsumer
    {
        // 回调接口
        private MessageReceiveCallBack _messageReceiveCallBack;
        
        // 队列名称
        private string _queueName;


        /// <summary>
        /// 构造消息接收者
        /// </summary>
        public MQReceiver()
            : base()
        {
        }


        /// <summary>
        /// 初始化接收者，如果初始化失败，将抛出异常
        /// </summary>
        /// <param name="aQueueName">队列名称</param>
        public void Init(string aQueueName)
        {
            _queueName = String.Format("queue://{0}", aQueueName);
            base.InitCore();
        }

        /// <summary>
        /// 初始化接收者，如果初始化失败，将抛出异常
        /// </summary>
        /// <param name="aQueueName">队列名称</param>
        /// <param name="mqConnection">MQ连接</param>
        public void Init(string aQueueName, MQConnection mqConnection)
        {
            _queueName = String.Format("queue://{0}", aQueueName);
            base.InitCore(mqConnection);
        }

        /// <summary>
        /// 同步接收消息
        /// </summary>
        /// <param name="aTimeout">超时毫秒数，0表示一直等待</param>
        /// <returns>MQ消息</returns>
        public MQMessage ReceiveMessage(int aTimeout)
        {
            return ReceiveMessage(aTimeout, String.Empty);
        }

        /// <summary>
        /// 同步接收消息
        /// </summary>
        /// <param name="aTimeout">超时毫秒数，0表示一直等待</param>
        /// <param name="aSelector">选择器</param>
        /// <returns>MQ消息</returns>
        public MQMessage ReceiveMessage(int aTimeout, string aSelector)
        {
            return Consume(_queueName, aTimeout, aSelector);
        }

        /// <summary>
        /// 异步接收消息
        /// </summary>
        /// <param name="aMessageReceiveCallBack">消息接收回调函数</param>
        public void RegisterMessageReceiver(MessageReceiveCallBack aMessageReceiveCallBack)
        {
             RegisterMessageReceiver(aMessageReceiveCallBack, string.Empty);
            //_messageReceiveCallBack = aMessageReceiveCallBack;
            //RegisterMessageListener(_queueName, this.OnMessageListener, String.Empty);
        }

        /// <summary>
        /// 异步接收消息
        /// </summary>
        /// <param name="aMessageReceiveCallBack">消息接收回调函数</param>
        public void RegisterMessageReceiver(MessageReceiveCallBack aMessageReceiveCallBack, string aSelector)
        {
            _messageReceiveCallBack = aMessageReceiveCallBack;
            RegisterMessageListener(_queueName, this.OnMessageListener, aSelector);
        }

        /// <summary>
        /// 从队列中读走消息，并关闭consume连接
        /// </summary>
        /// <param name="aTimeout">超时毫秒数，0表示一直等待</param>
        /// <param name="aSelector">选择器</param>
        /// <returns>MQ消息</returns>
        public MQMessage ReceiveAndRemoveMessage(int aTimeout, string aSelector)
        {
            MQMessage msg = Consume(_queueName, aTimeout, aSelector);
            UnConsume(_queueName, aSelector);
            return msg;
        }

        /// <summary>
        /// 处理已订阅主题的回调函数
        /// </summary>
        /// <param name="msg">已订阅的消息</param>
        private void OnMessageListener(IMessage msg)
        {
            if (msg != null)
            {
                try
                {
                    MQMessage mqMessage = new MQMessage();
                    mqMessage.ParseMessage(msg);
                    if (_messageReceiveCallBack != null)
                        _messageReceiveCallBack(mqMessage);
                }
                catch (Exception ex)
                {
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, String.Format("{0}.OnMessageCallback Exception:\n{1}", GetType().FullName, ex));
                }
            }
        }
    }
}
