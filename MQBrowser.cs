/*********************************************************************
 * 名称： MQBrowser.cs
 * 功能： 消息浏览者
 * 作者： 廖洪周
 * 公司： xQuant
 * 日期： 2009.04.14
 * 最新： 
 * 版本： 1.0.0
 * 修订：
 *        1.0.0 --- 创建代码
 *********************************************************************/

using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// 浏览者
    /// </summary>
    public class MQBrowser : MQSession
    {
        // 队列名称
        private string _queueName = null;

        // XMS的浏览者对象
        private IQueueBrowser _browser;

        // 目的地
        private IDestination _destination;

        /// <summary>
        /// 构造浏览者
        /// </summary>
        public MQBrowser()
        {
        }

        /// <summary>
        /// 初始化浏览者，如果初始化失败，将抛出异常
        /// </summary>
        /// <param name="aQueueName">队列名称</param>
        public void Init(string aQueueName)
        {
            _queueName = String.Format("queue:///{0}", aQueueName);
            base.InitCore();
        }

        /// <summary>
        /// 初始化浏览者，如果初始化失败，将抛出异常
        /// </summary>
        /// <param name="aQueueName">队列名称</param>
        /// <param name="mqConnection">MQ连接</param>
        public void Init(string aQueueName, MQConnection mqConnection)
        {
            _queueName = String.Format("queue:///{0}", aQueueName);
            base.InitCore(mqConnection);
        }

        /// <summary>
        /// 打开浏览者
        /// </summary>
        protected override void Open()
        {
            // 先打开Session
            base.Open();

            // 创建目的地
            _destination = Session.CreateQueue(_queueName);
            _destination.SetIntProperty(XMSC.DELIVERY_MODE, XMSC.DELIVERY_PERSISTENT);

            // 创建浏览者
            _browser = Session.CreateBrowser(_destination);
        }

        /// <summary>
        /// 重新打开浏览者
        /// </summary>
        internal override void ReOpen()
        {
            this.Open();
        }

        /// <summary>
        /// 关闭浏览者，释放资源
        /// </summary>
        internal override void ReleaseResource()
        {
            // 关闭浏览者
            CloseBrowser();

            // 释放资源
            base.ReleaseResource();
        }

        /// <summary>
        /// 关闭订阅者
        /// </summary>
        private void CloseBrowser()
        {
            try
            {
                if (_browser != null)
                {
                    _browser.Close();
                    _browser.Dispose();
                }
            }
            catch
            {
                // 可能原先的连接已断开，可忽略此异常。
            }

            _browser = null;
        }


        /// <summary>
        /// 同步浏览消息
        /// </summary>
        /// <returns>返回消息迭代器</returns>
        public IEnumerator BrowserMessage()
        {
            return _browser.GetEnumerator();
        }
    }
}
