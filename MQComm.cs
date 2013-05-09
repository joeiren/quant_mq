/*********************************************************************
 * 名称： MQComm.cs
 * 功能： MQ通讯模块
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.12
 * 最新： 2007.11.21
 * 版本： 1.2.0
 * 修订：
 *        1.0.0 --- 支持WebShpere MQ下的发布/订阅主题
 *        1.1.0 --- 支持WebShpere MQ下的发送/接收消息
 *        1.2.0 --- 廖洪周重构代码 By 2009-04-10
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.CompilerServices;
using System.Collections;
using System.Configuration;
using IBM.XMS;

namespace xQuant.MQ
{

    /// <summary>
    /// 主题订阅回调委托
    /// </summary>
    /// <param name="aData">主题内容</param>
    public delegate void TopicSubscribeCallBack(object aData);


    /// <summary>
    /// 消息接收回调委托
    /// </summary>
    /// <param name="aMessage">消息</param>
    public delegate void MessageReceiveCallBack(MQMessage aMessage);



    /// <summary>
    /// MQ常量定义
    /// </summary>
    public class MQConst
    {
        /// <summary>
        /// JSM消息头用户自定义块
        /// </summary>
        public const string JMS_FLAG = "JMS";
        
        /// <summary>
        /// 服务ID
        /// </summary>
        public const string USER_SERVICEID = "U_SERVICEID";

        /// <summary>
        /// 任务编码
        /// </summary>
        public const string USER_TASKCODE = "U_TASKCODE";

        /// <summary>
        /// 服务状态
        /// </summary>
        public const string USER_SERVICESTATUS = "U_SERVICESTATUS";

        /// <summary>
        /// 进度，最大值为100
        /// </summary>
        public const string USER_SERVICEGUAGE = "U_SERVICEGUAGE";

        /// <summary>
        /// 进度信息
        /// </summary>
        public const string USER_SERVICEGUAGEINFO = "U_SERVICEGUAGEINFO";

        /// <summary>
        /// 基准日期
        /// </summary>
        public const string USER_BASEDATE = "U_BASEDATE";
        
        /// <summary>
        /// 用户ID
        /// </summary>
        public const string USER_USERID = "U_USERID";

        /// <summary>
        /// 发送者消息的默认有效时间
        /// </summary>
        public const long MsgTimeToLive = 1800000;
    }


    /// <summary>
    /// MQ参数信息类
    /// </summary>
    /// <typeparam name="T">int or string</typeparam>
    public class MQParameter<T>
    {
        /// <summary>
        /// 构造MQ参数信息类
        /// </summary>
        /// <param name="name">参数名称</param>
        public MQParameter(string name)
        {
            _name = name;
        }

        /// <summary>
        /// 构造MQ参数信息类
        /// </summary>
        /// <param name="name">参数名称</param>
        /// <param name="defVal">参数值</param>
        public MQParameter(string name, T defVal)
        {
            _name = name;
            _value = defVal;
        }

        /// <summary>
        /// 参数名称
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// 参数值
        /// </summary>
        public T Value
        {
            get { return _value; }
            set { _value = value; }
        }

        private readonly string _name;
        private T _value;
    }


    /// <summary>
    /// MQ Session，每个Session对应一个线程
    /// </summary>
    public class MQSession
    {
        // Session名称
        private string _sessionName;

        // MQ连接
        private MQConnection _mqConnection;

        // XMS的Session对象
        private ISession _session;


        /// <summary>
        /// 构造MQSession对象
        /// </summary>
        public MQSession()
        {
            _sessionName = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Session名称
        /// </summary>
        public string SessionName { get { return _sessionName; } }

        /// <summary>
        /// 连接
        /// </summary>
        public MQConnection Connection { get { return _mqConnection; } }
       
        /// <summary>
        /// XMS的Session对象
        /// </summary>
        internal ISession Session { get { return _session; } }


        /// <summary>
        /// 初始化，并打开Session，使用缺省的MQ连接
        /// </summary>
        protected void InitCore()
        {
            InitCore(MQConnection.Default);
        }
     
        /// <summary>
        /// 初始化，并打开Session
        /// </summary>
        /// <param name="mqConnection">MQ连接对象</param>
        protected void InitCore(MQConnection mqConnection)
        {
            // 向连接里增加Session
            mqConnection.AddSession(this);

            // 设置MQ连接
            _mqConnection = mqConnection;

            // 打开Session
            Open();
        }

        /// <summary>
        /// 打开发布和订阅者
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        protected virtual void Open()
        {
            if (!Connection.Connected)
                throw new Exception("尚未建立MQ连接，无法打开Session！");

            // 创建Session
            _session = _mqConnection.CreateSession();
        }

        /// <summary>
        /// 重新打开发布和订阅者
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        internal virtual void ReOpen()
        {
            // 创建Session
            _session = _mqConnection.CreateSession();
        }

        /// <summary>
        /// 关闭发布/订阅
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        public void Close()
        {
            // 从连接的Session列表里移除自己
            if (_mqConnection != null)
                _mqConnection.RemoveSession(this);

            // 释放资源
            Release();
        }

        /// <summary>
        /// 子类重载该方法，在关闭时释放资源
        /// </summary>
        internal virtual void Release()
        {
            ReleaseResource();
        }

        /// <summary>
        /// 子类重载该方法，释放资源
        /// </summary>
        internal virtual void ReleaseResource()
        {
            // 关闭Session
            if (_session != null)
            {
                try
                {
                    _session.Close();
                    _session.Dispose();
                }
                catch
                {
                    // 可能连接已经断开（在连接断开的时候再次调用Close方法会产生异常），这里的异常不用处理。
                }
                _session = null;
            }
        }
    }


    /// <summary>
    /// 订阅者，订阅者可以订阅多个主题
    /// </summary>
    public class MQConsumer : MQSession
    {
        private bool _isDurable = true;
        /// <summary>
        /// 是否是可持久化的订阅模式。
        /// </summary>
        public bool IsDurable
        {
            get
            {
                return _isDurable;
            }
        }

        private Dictionary<string, TopicSub> _topics = new Dictionary<string, TopicSub>();

        /// <summary>
        /// 构造订阅者
        /// </summary>
        protected MQConsumer()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="aIsDurable">是否是可持久化的订阅模式</param>
        protected MQConsumer(bool aIsDurable)
        {
            _isDurable = aIsDurable;
        }

        /// <summary>
        /// 重新打开订阅者
        /// </summary>
        internal override void ReOpen()
        {
            // 先打开Session
            base.ReOpen();
            
            // 更新所有的TopicSub，重新订阅
            foreach (KeyValuePair<string, TopicSub> entry in _topics)
            {
                // 打开每一个订阅者，由于使用Durable订阅者，这里不先进行关闭操作。
                try
                {
                    entry.Value.Open();
                }
                catch (Exception ex)
                {
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, string.Format("打开{0}失败，相关主题为：{1}，异常为：{2}", GetType().FullName, entry.Value.Topic, ex.ToString()));

                    if (ex.Message.IndexOf(IBM.XMS.MQC.MQRCCF_SUBSCRIPTION_LOCKED.ToString()) >= 0)
                    {
                        Connection.RaiseConnectionFailed(FailReasion.TopicLocked);
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        internal override void Release()
        {
            base.Release();

            lock (_topics)
            {
                _topics.Clear();
            }
        }

        /// <summary>
        /// 关闭订阅者，释放资源
        /// </summary>
        internal override void ReleaseResource()
        {
            // 关闭订阅者
            CloseConsumer();

            // 释放资源
            base.ReleaseResource();
        }

        /// <summary>
        /// 关闭订阅者
        /// </summary>
        private void CloseConsumer()
        {
            lock (_topics)
            {
                foreach (KeyValuePair<string, TopicSub> entry in _topics)
                {
                    entry.Value.Close();
                }
            }
        }


        /// <summary>
        /// 获取主题，同步方式
        /// </summary>
        /// <param name="aTopic">主题</param>
        /// <param name="aTimeout">超时毫秒数，0表示一直等待</param>
        /// <returns>结果</returns>
        public byte[] Consume(string aTopic, int aTimeout)
        {
            byte[] objGraph = null;

            // get topic
            TopicSub topicSub = GetTopic(aTopic, String.Empty);

            // recvive message
            IMessage msg = topicSub.Consumer.Receive(aTimeout);
            if (msg != null)
            {
                IObjectMessage objMsg = msg as IObjectMessage;
                if (objMsg != null)
                    objGraph = objMsg.GetObject();
            }

            return objGraph;
        }

        /// <summary>
        /// 获取消息，同步方式
        /// </summary>
        /// <param name="aTopic">队列</param>
        /// <param name="aTimeout">超时毫秒数，0表示一直等待</param>
        /// <param name="aSelector">选择器</param>
        /// <returns>MQ消息</returns>
        public MQMessage Consume(string aTopic, int aTimeout, string aSelector)
        {
            MQMessage mqMessage = null;

            // get topic
            TopicSub topicSub = GetTopic(aTopic, aSelector);

            // recvive message
            IMessage msg = topicSub.Consumer.Receive(aTimeout);
            if (msg != null)
            {
                mqMessage = new MQMessage();
                mqMessage.ParseMessage(msg);
            }

            return mqMessage;
        }

        /// <summary>
        /// 注册消息侦听器，异步回调函数方式
        /// </summary>
        /// <param name="aTopic">主题或队列</param>
        /// <param name="aMessageListener">消息侦听器</param>
        /// <param name="aSelector">选择器</param>
        internal void RegisterMessageListener(string aTopic, MessageListener aMessageListener, string aSelector)
        {
            //get topic
            TopicSub topicSub = GetTopic(aTopic, aSelector);

            // register message callback
            topicSub.RegisterMessageListener(aMessageListener);
        }

        /// <summary>
        /// 取消
        /// </summary>
        /// <param name="aTopic">主题/队列</param>
        /// <param name="aSelector">选择器</param>
        internal void UnConsume(string aTopic, string aSelector)
        {
            RemoveTopic(aTopic, aSelector);
        }

        // 获取消息主题
        private TopicSub GetTopic(string aTopic, string aSelector)
        {
            TopicSub topicSub = null;

            lock (_topics)
            {
                string subscription = String.IsNullOrEmpty(aSelector) ? aTopic : aTopic + aSelector;

                if (_topics.ContainsKey(subscription))
                {
                    topicSub = _topics[subscription];
                    if (topicSub.Consumer == null)
                    {
                        topicSub = null;
                        _topics.Remove(subscription);
                    }
                }


                if (topicSub == null)
                {
                    topicSub = new TopicSub(aTopic, aSelector, this);                    
                    _topics.Add(subscription, topicSub);

                    try
                    {
                        topicSub.Open();
                    }
                    catch (XMSException ex)
                    {
                        if (ex.LinkedException != null)
                            throw new Exception(String.Format("订阅主题：{0}失败，{1}", aTopic, ex.LinkedException.ToString()));
                        else
                            throw ex;
                    }
                }
            }
            
            return topicSub;
        }

        // 移除并关闭消息主题
        private void RemoveTopic(string aTopic, string aSelector)
        {
            TopicSub topicSub = null;

            // 移除主题
            lock (_topics)
            {
                string subscription = String.IsNullOrEmpty(aSelector) ? aTopic : aTopic + aSelector;
                if (_topics.ContainsKey(subscription))
                {
                    topicSub = _topics[subscription];
                    _topics.Remove(subscription);
                }
            }

            // 关闭主题
            if (topicSub != null)
                topicSub.Close();
        }        


        /// <summary>
        /// 主题订阅
        /// </summary>
        class TopicSub
        {
            // 订阅者
            private MQConsumer _mqConsumer;

            private string _topic;

            private string _selector;

            private IDestination _destination;

            private IMessageConsumer _consumer;           

            // 消息侦听
            private MessageListener _messageListener;


            /// <summary>
            /// 构造主题订阅
            /// </summary>
            /// <param name="topic">主题</param>
            /// <param name="selector">过滤条件</param>
            /// <param name="mqConsumer">消费者</param>
            public TopicSub(string topic, string selector, MQConsumer mqConsumer)
            {
                _topic = topic;
                _selector = selector;
                _mqConsumer = mqConsumer;
            }

            /// <summary>
            /// 主题或消息
            /// </summary>
            public string Topic { get { return _topic; } }
            
            /// <summary>
            /// 过滤器
            /// </summary>
            public string Selector { get { return _selector; } }
                        
            /// <summary>
            /// 目的地地址
            /// </summary>
            public IDestination Destination { get { return _destination; } }
            
            /// <summary>
            /// 订阅者
            /// </summary>
            public IMessageConsumer Consumer { get { return _consumer; } }

            /// <summary>
            /// 订阅者的标识
            /// </summary>
            public string Subscription { get { return _selector == null ? _topic : _topic + _selector; } }
            
            /// <summary>
            /// 注册订阅主题
            /// </summary>
            public void Open()
            {
                // 创建目的地
                if (_topic.StartsWith("topic://"))
                    _destination = _mqConsumer.Session.CreateTopic(_topic);
                else
                    _destination = _mqConsumer.Session.CreateQueue(_topic);
                _destination.SetIntProperty(XMSC.DELIVERY_MODE, XMSC.DELIVERY_PERSISTENT);

                // 创建消费者
                if (_topic.StartsWith("topic://") && _mqConsumer.IsDurable)
                {
                    _consumer = _mqConsumer.Session.CreateDurableSubscriber(_destination, Subscription, _selector, false);
                }
                else
                {
                    _consumer = _mqConsumer.Session.CreateConsumer(_destination,_selector);
                }

                if (_messageListener != null)
                {
                    RegisterMessageListener(_messageListener);
                }
            }

            /// <summary>
            /// 关闭订阅主题
            /// </summary>
            public void Close()
            {
                // 先关闭消费者，才能成功注销订阅

                // 关闭消费者
                CloseConsumer();

                // 注销订阅
                Unsubscribe();
            }

            // 关闭消费者
            private void CloseConsumer()
            {
                try
                {
                    if (_consumer != null)
                    {
                        _consumer.MessageListener = null;
                        _consumer.Close();
                        _consumer.Dispose();
                    }
                }
                catch
                {
                    // 可能原先的连接已断开，可忽略此异常。
                }
                _consumer = null;
            }

            // 注销订阅
            private void Unsubscribe()
            {
                try
                {
                    // 注销订阅。由MQ服务器删除已放入的消息并删除订阅者。
                    if (_topic.StartsWith("topic://") && _mqConsumer.IsDurable)
                        _mqConsumer.Session.Unsubscribe(Subscription);
                }
                catch
                {
                    // 可能原先的连接已断开，可忽略此异常。
                }
            }

            /// <summary>
            /// 注册消息侦听器
            /// </summary>
            /// <param name="messageListener">消息侦听器</param>
            public void RegisterMessageListener(MessageListener messageListener)
            {
                _messageListener = messageListener;
                if (_messageListener != null && Consumer != null)
                    Consumer.MessageListener = _messageListener;
            }
        }
    }



    /// <summary>
    /// 发布者
    /// </summary>
    public class MQProducer : MQSession
    {
        // 缓存发布的目的地字典
        Dictionary<string, IDestination> _destinations = new Dictionary<string, IDestination>();

        // XMS发布者对象
        private IMessageProducer _producer;

        // 消息过期时间
        private long _timeToLive = 0;

        /// <summary>
        /// 构造发布者，设置消息过期时间为0，表示永不过期
        /// </summary>
        protected MQProducer()
            : this(0)
        {
        }

        /// <summary>
        /// 构造发布者
        /// </summary>
        protected MQProducer(long timeToLive)
        {
            _timeToLive = timeToLive;
        }

        /// <summary>
        /// 打开发布者
        /// </summary>
        protected override void Open()
        {
            // 先打开Session
            base.Open();

            // 创建XMS对象
            _producer = Session.CreateProducer(null);
        }

        /// <summary>
        /// 重新打开发布者
        /// </summary>
        internal override void ReOpen()
        {
            // 清理缓存的目的地信息，由于Session更新，这些目的地可能已经失效了
            lock (_destinations)
            {
                _destinations.Clear();
            }

            this.Open();
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        internal override void ReleaseResource()
        {
            // 关闭发布者
            CloseProducer();

            // 释放Session
            base.ReleaseResource();
        }

        /// <summary>
        /// 关闭发布者
        /// </summary>
        private void CloseProducer()
        {
            try
            {
                // 清理缓存的目的地
                lock (_destinations)
                {
                    _destinations.Clear();
                }

                if (_producer != null)
                {
                    _producer.Close();
                    _producer.Dispose();
                }
            }
            catch
            {
                // 关闭失败，可能是由于网络原因，可忽略此异常
            }

            _producer = null;
        }

        /// <summary>
        /// 发送主题消息
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="objGraph">内容</param>
        protected void Produce(string topic, byte[] objGraph)
        {
            Produce(topic, objGraph, true);
        }

        /// <summary>
        /// 发送主题消息
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="objGraph">内容</param>
        /// <param name="retry">重试次数</param>
        private void Produce(string topic, byte[] objGraph, bool retry)
        {
            try
            {
                // 根据主题获取目的地
                IDestination dest = GetTopic(topic);

                // 创建消息
                IObjectMessage msg = Session.CreateObjectMessage();
                msg.SetObject(objGraph);

                // 发送消息
                _producer.Send(dest, msg, DeliveryMode.Persistent, _producer.Priority, _timeToLive);
            }
            catch (Exception ex)
            {
                if (retry)
                {
                    // 重新连接，然后再发送消息
                    Connection.ReOpen();
                    if (Connection.Connected)
                        Produce(topic, objGraph, false);
                    else
                        throw ex;
                }
                else
                    throw ex;
            }
        }

        /// <summary>
        /// 发送主题消息
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="aMessage">MQ消息</param>
        protected void Produce(string topic, MQMessage aMessage)
        {
            Produce(topic, aMessage, true);
        }

        /// <summary>
        /// 发送主题消息
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="aMessage">MQ消息</param>
        /// <param name="retry">重试次数</param>
        private void Produce(string topic, MQMessage aMessage, bool retry)
        {
            try
            {
                string s = string.Format("aMessage={0}", aMessage);
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Debug, s);
                // 根据主题获取目的地
                IDestination dest = GetTopic(topic);

                // 创建消息
                IMessage msg;
                if (aMessage.Text != null)
                {
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Debug, "Text!=null");
                    msg = Session.CreateTextMessage(aMessage.Text);
                    
                }
                else
                {
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Debug, "Text==null");
                    msg = Session.CreateObjectMessage();
                    ((IObjectMessage)msg).SetObject(aMessage.Byte);                    
                }

                // 转换信息头
                if (msg != null)
                {
                    aMessage.ToMessage(msg);
                }
                s = string.Format("dest={0},msg={1},_producer.Priority={2},_timeToLive={3}"
                    , dest, msg, _producer.Priority, _timeToLive);
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Debug, s);
                // 发送消息
                _producer.Send(dest, msg, DeliveryMode.Persistent, _producer.Priority, _timeToLive);
            }
            catch (Exception ex)
            {

                string s = string.Format("topic={0},aMessage={1},retry={2},IDestination={3},_producer={4},Session={5}\r\nexception={6}"
                    , topic, aMessage, retry, GetTopic(topic), _producer, Session,ex.Message);
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, s);
                if (retry)
                {
                    // 重新连接，然后再发送消息
                    Connection.ReOpen();
                    if (Connection.Connected)
                        Produce(topic, aMessage, false);
                    else
                        throw ex;
                }
                else
                    throw ex;
            }
        }

        /// <summary>
        /// 从缓存中获取目的地，如果还没有被缓存，则创建新的对象并缓存起来
        /// </summary>
        /// <param name="topic">主题</param>
        /// <returns>返回主题对应的IDestination对象</returns>
        private IDestination GetTopic(string topic)
        {
            IDestination dest = null;
            
            lock (_destinations)
            {
                if (!_destinations.ContainsKey(topic))
                {
                    dest = CreateDestination(topic);
                    if (dest != null)
                        _destinations.Add(topic, dest);
                }
                else
                    dest = _destinations[topic];
            }
            
            return dest;
        }

        /// <summary>
        /// 根据主题创建IDestination对象
        /// </summary>
        /// <param name="topic">主题</param>
        /// <returns>返回创建的IDestination对象</returns>
        private IDestination CreateDestination(string topic)
        {
            IDestination dest = null;

            if (Session != null)
            {
                if (topic.StartsWith("topic://"))
                    dest = Session.CreateTopic(topic);
                else
                    dest = Session.CreateQueue(topic);

                dest.SetIntProperty(XMSC.DELIVERY_MODE, XMSC.DELIVERY_PERSISTENT);
            }

            return dest;
        }
    }
}
