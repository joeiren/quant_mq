/*********************************************************************
 * 名称： MQSubscriber.cs
 * 功能： 主题订阅者
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.21
 * 最新： 
 * 版本： 1.1.0
 * 修订：
 *        1.0.0 --- 以同步/异步方式订阅主题
 *        1.1.0 --- 廖洪周重构代码，By 2009-04-11
 *********************************************************************/
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// 客户端订阅者
    /// </summary>
    public class MQSubscriber : MQConsumer
    {
        private Dictionary<string, MQTopicCallBack> _callbackDict;
        private BinaryFormatter _formatter = new BinaryFormatter();

        /// <summary>
        /// 已注册的主题数
        /// </summary>
        public int TopicCount { get { return _callbackDict.Count; } }


        /// <summary>
        /// 构造订阅者
        /// </summary>
        public MQSubscriber()
            : base()
        {
            _callbackDict = new Dictionary<string, MQTopicCallBack>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="aIsDurable">是否是可持久化的订阅模式</param>
        public MQSubscriber(bool aIsDurable)
            : base(aIsDurable)
        {
            _callbackDict = new Dictionary<string, MQTopicCallBack>();
        }


        /// <summary>
        /// 初始化订阅者，如果初始化失败，将抛出异常
        /// </summary>
        public void Init()
        {
            base.InitCore();
        }

        /// <summary>
        /// 初始化订阅者，如果初始化失败，将抛出异常
        /// </summary>
        /// <param name="mqConnection">MQ连接</param>
        public void Init(MQConnection mqConnection)
        {
            base.InitCore(mqConnection);
        }

        /// <summary>
        /// 同步接收主题
        /// </summary>
        /// <param name="aTopic">主题</param>
        /// <param name="aTimeout">超时毫秒数，0表示一直等待</param>
        /// <returns>主题数据</returns>
        public object ReceiveTopic(string aTopic, int aTimeout)
        {
            byte[] bData = Consume(String.Format("topic://{0}", aTopic), aTimeout);

            if (bData == null || bData.Length == 0)
                return null;

            MemoryStream stream = new MemoryStream(bData);
            stream.Position = 0;

            object objData = _formatter.Deserialize(stream);
            return objData;
        }

        /// <summary>
        /// 注册订阅主题
        /// </summary>
        /// <param name="aTopic">主题</param>
        /// <param name="aTopicSubscribeCallBack">主题订阅回调函数</param>
        public void RegisterTopic(MQTopic aTopic, TopicSubscribeCallBack aTopicSubscribeCallBack)
        {
            if (String.IsNullOrEmpty(aTopic.TopicName))
                return;

            MQTopicCallBack mqTopicCallBack = null;

            lock (_callbackDict)
            {
                if (_callbackDict.ContainsKey(aTopic.TopicName))
                    return;

                mqTopicCallBack = new MQTopicCallBack(aTopic, aTopicSubscribeCallBack);
                _callbackDict.Add(aTopic.TopicName, mqTopicCallBack);
            }

            if (mqTopicCallBack != null)
            {
                RegisterMessageListener(String.Format("topic://{0}", aTopic.TopicName), mqTopicCallBack.OnMessageListener, String.Empty);
            }
        }

        /// <summary>
        /// 注册主题列表
        /// </summary>
        /// <param name="aList">主题列表</param>
        /// <param name="aTopicSubscribeCallBack">主题订阅回调函数</param>
        public void RegisterTopic(List<MQTopic> aList, TopicSubscribeCallBack aTopicSubscribeCallBack)
        {
            // 先停用，后注册主题比较快
            Connection.Stop();
            try
            {
                // 增加所有主题
                foreach (MQTopic topic in aList)
                {
                    RegisterTopic(topic, aTopicSubscribeCallBack);
                }

                // 删除已注册但列表中没有的主题
                lock (_callbackDict)
                {
                    List<MQTopic> list = new List<MQTopic>();

                    foreach (MQTopicCallBack mqTopicCallBack in _callbackDict.Values)
                    {
                        if (!aList.Contains(mqTopicCallBack.Topic))
                        {
                            list.Add(mqTopicCallBack.Topic);
                        }
                    }

                    // 从列表里移除主题
                    foreach (MQTopic topic in list)
                    {
                        UnRegisterTopic(topic);
                    }

                    list.Clear();
                }
            }
            finally
            {
                Connection.Start();
            }
        }

        /// <summary>
        /// 取消订阅主题
        /// </summary>
        /// <param name="aTopic">主题</param>
        public void UnRegisterTopic(MQTopic aTopic)
        {
            lock (_callbackDict)
            {
                _callbackDict.Remove(aTopic.TopicName);
            }

            // 使得可以取消同步/异步下的订阅
            UnConsume(String.Format("topic://{0}", aTopic.TopicName), String.Empty);
        }

        /// <summary>
        /// 判断是否存在需要修改的主题
        /// </summary>
        /// <param name="listTopic">新主题列表</param>
        /// <returns>结果</returns>
        public bool HasChangedTopic(List<MQTopic> listTopic)
        {
            lock (_callbackDict)
            {
                if (listTopic.Count != _callbackDict.Count)
                    return true;

                foreach (MQTopic topic in listTopic)
                {
                    if (!_callbackDict.ContainsKey(topic.TopicName))
                        return true;
                }

                return false;
            }
        }

        /// <summary>
        /// 判断主题是否已经注册
        /// </summary>
        /// <param name="topic">主题</param>
        /// <returns>结果</returns>
        public bool IsRegistered(MQTopic topic)
        {
            lock (_callbackDict)
            {
                return _callbackDict.ContainsKey(topic.TopicName);
            }
        }


        /// <summary>
        /// 释放资源
        /// </summary>
        internal override void ReleaseResource()
        {
            lock (_callbackDict)
            {
                _callbackDict.Clear();
            }

            base.ReleaseResource();
        }
    }

    internal class MQTopicCallBack
    {
        private MQTopic _topic = null;
        private TopicSubscribeCallBack _topicSubscribeCallBack;
        private BinaryFormatter _formatter = new BinaryFormatter();

        public MQTopicCallBack(MQTopic aTopic, TopicSubscribeCallBack aTopicSubscribeCallBack)
        {
            _topic = aTopic;
            _topicSubscribeCallBack = aTopicSubscribeCallBack;
        }

        public MQTopic Topic
        {
            get { return _topic; }
        }

        /// <summary>
        /// 处理已订阅主题的回调函数
        /// </summary>
        /// <param name="msg">已订阅的消息</param>
        public void OnMessageListener(IMessage msg)
        {
            try
            {
                IObjectMessage objMsg = (IObjectMessage)msg;
                byte[] bData = objMsg.GetObject();
                if (bData == null || bData.Length == 0)
                    return;

                MemoryStream stream = new MemoryStream(bData);
                stream.Position = 0;

                object objData = _formatter.Deserialize(stream);

                if (_topicSubscribeCallBack != null)
                    _topicSubscribeCallBack(objData);
            }
            catch (Exception ex)
            {
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, String.Format("{0}.OnMessageCallback Exception:\n{1}", GetType().FullName, ex));
            }
        }
    }
}
