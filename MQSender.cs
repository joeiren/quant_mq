/*********************************************************************
 * 名称： MQSender.cs
 * 功能： 消息发送者
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.21
 * 最新： 
 * 版本： 1.1.0
 * 修订：
 *        1.0.0 --- 向指定的消息队列发送消息
 *        1.1.0 --- 廖洪周重构代码，By 2009-04-11
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace xQuant.MQ
{
    /// <summary>
    /// 发送者
    /// </summary>
    public class MQSender : MQProducer
    {
        // 队列名称
        private string _queueName = null;

        /// <summary>
        /// 构造发送者
        /// </summary>
        public MQSender()
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
        /// 发送消息，发送失败，将抛出异常
        /// </summary>
        /// <param name="aMessage">MQ消息</param>
        public void SendMessage(MQMessage aMessage)
        {
            try
            {
                Produce(_queueName, aMessage);
            }
            catch(Exception exp)
            {
                Log4.LogHelper.Write(Log4.LogLevel.Error, exp.ToString());
                throw new Exception("发送MQ消息失败，可能是网络出现问题或MQ正在重连中，请稍候再试。");
            }
        }
    }
}
