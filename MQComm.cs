/*********************************************************************
 * ���ƣ� MQComm.cs
 * ���ܣ� MQͨѶģ��
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.12
 * ���£� 2007.11.21
 * �汾�� 1.2.0
 * �޶���
 *        1.0.0 --- ֧��WebShpere MQ�µķ���/��������
 *        1.1.0 --- ֧��WebShpere MQ�µķ���/������Ϣ
 *        1.2.0 --- �κ����ع����� By 2009-04-10
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
    /// ���ⶩ�Ļص�ί��
    /// </summary>
    /// <param name="aData">��������</param>
    public delegate void TopicSubscribeCallBack(object aData);


    /// <summary>
    /// ��Ϣ���ջص�ί��
    /// </summary>
    /// <param name="aMessage">��Ϣ</param>
    public delegate void MessageReceiveCallBack(MQMessage aMessage);



    /// <summary>
    /// MQ��������
    /// </summary>
    public class MQConst
    {
        /// <summary>
        /// JSM��Ϣͷ�û��Զ����
        /// </summary>
        public const string JMS_FLAG = "JMS";
        
        /// <summary>
        /// ����ID
        /// </summary>
        public const string USER_SERVICEID = "U_SERVICEID";

        /// <summary>
        /// �������
        /// </summary>
        public const string USER_TASKCODE = "U_TASKCODE";

        /// <summary>
        /// ����״̬
        /// </summary>
        public const string USER_SERVICESTATUS = "U_SERVICESTATUS";

        /// <summary>
        /// ���ȣ����ֵΪ100
        /// </summary>
        public const string USER_SERVICEGUAGE = "U_SERVICEGUAGE";

        /// <summary>
        /// ������Ϣ
        /// </summary>
        public const string USER_SERVICEGUAGEINFO = "U_SERVICEGUAGEINFO";

        /// <summary>
        /// ��׼����
        /// </summary>
        public const string USER_BASEDATE = "U_BASEDATE";
        
        /// <summary>
        /// �û�ID
        /// </summary>
        public const string USER_USERID = "U_USERID";

        /// <summary>
        /// ��������Ϣ��Ĭ����Чʱ��
        /// </summary>
        public const long MsgTimeToLive = 1800000;
    }


    /// <summary>
    /// MQ������Ϣ��
    /// </summary>
    /// <typeparam name="T">int or string</typeparam>
    public class MQParameter<T>
    {
        /// <summary>
        /// ����MQ������Ϣ��
        /// </summary>
        /// <param name="name">��������</param>
        public MQParameter(string name)
        {
            _name = name;
        }

        /// <summary>
        /// ����MQ������Ϣ��
        /// </summary>
        /// <param name="name">��������</param>
        /// <param name="defVal">����ֵ</param>
        public MQParameter(string name, T defVal)
        {
            _name = name;
            _value = defVal;
        }

        /// <summary>
        /// ��������
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// ����ֵ
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
    /// MQ Session��ÿ��Session��Ӧһ���߳�
    /// </summary>
    public class MQSession
    {
        // Session����
        private string _sessionName;

        // MQ����
        private MQConnection _mqConnection;

        // XMS��Session����
        private ISession _session;


        /// <summary>
        /// ����MQSession����
        /// </summary>
        public MQSession()
        {
            _sessionName = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Session����
        /// </summary>
        public string SessionName { get { return _sessionName; } }

        /// <summary>
        /// ����
        /// </summary>
        public MQConnection Connection { get { return _mqConnection; } }
       
        /// <summary>
        /// XMS��Session����
        /// </summary>
        internal ISession Session { get { return _session; } }


        /// <summary>
        /// ��ʼ��������Session��ʹ��ȱʡ��MQ����
        /// </summary>
        protected void InitCore()
        {
            InitCore(MQConnection.Default);
        }
     
        /// <summary>
        /// ��ʼ��������Session
        /// </summary>
        /// <param name="mqConnection">MQ���Ӷ���</param>
        protected void InitCore(MQConnection mqConnection)
        {
            // ������������Session
            mqConnection.AddSession(this);

            // ����MQ����
            _mqConnection = mqConnection;

            // ��Session
            Open();
        }

        /// <summary>
        /// �򿪷����Ͷ�����
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        protected virtual void Open()
        {
            if (!Connection.Connected)
                throw new Exception("��δ����MQ���ӣ��޷���Session��");

            // ����Session
            _session = _mqConnection.CreateSession();
        }

        /// <summary>
        /// ���´򿪷����Ͷ�����
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        internal virtual void ReOpen()
        {
            // ����Session
            _session = _mqConnection.CreateSession();
        }

        /// <summary>
        /// �رշ���/����
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        public void Close()
        {
            // �����ӵ�Session�б����Ƴ��Լ�
            if (_mqConnection != null)
                _mqConnection.RemoveSession(this);

            // �ͷ���Դ
            Release();
        }

        /// <summary>
        /// �������ظ÷������ڹر�ʱ�ͷ���Դ
        /// </summary>
        internal virtual void Release()
        {
            ReleaseResource();
        }

        /// <summary>
        /// �������ظ÷������ͷ���Դ
        /// </summary>
        internal virtual void ReleaseResource()
        {
            // �ر�Session
            if (_session != null)
            {
                try
                {
                    _session.Close();
                    _session.Dispose();
                }
                catch
                {
                    // ���������Ѿ��Ͽ��������ӶϿ���ʱ���ٴε���Close����������쳣����������쳣���ô���
                }
                _session = null;
            }
        }
    }


    /// <summary>
    /// �����ߣ������߿��Զ��Ķ������
    /// </summary>
    public class MQConsumer : MQSession
    {
        private bool _isDurable = true;
        /// <summary>
        /// �Ƿ��ǿɳ־û��Ķ���ģʽ��
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
        /// ���충����
        /// </summary>
        protected MQConsumer()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="aIsDurable">�Ƿ��ǿɳ־û��Ķ���ģʽ</param>
        protected MQConsumer(bool aIsDurable)
        {
            _isDurable = aIsDurable;
        }

        /// <summary>
        /// ���´򿪶�����
        /// </summary>
        internal override void ReOpen()
        {
            // �ȴ�Session
            base.ReOpen();
            
            // �������е�TopicSub�����¶���
            foreach (KeyValuePair<string, TopicSub> entry in _topics)
            {
                // ��ÿһ�������ߣ�����ʹ��Durable�����ߣ����ﲻ�Ƚ��йرղ�����
                try
                {
                    entry.Value.Open();
                }
                catch (Exception ex)
                {
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, string.Format("��{0}ʧ�ܣ��������Ϊ��{1}���쳣Ϊ��{2}", GetType().FullName, entry.Value.Topic, ex.ToString()));

                    if (ex.Message.IndexOf(IBM.XMS.MQC.MQRCCF_SUBSCRIPTION_LOCKED.ToString()) >= 0)
                    {
                        Connection.RaiseConnectionFailed(FailReasion.TopicLocked);
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// �ͷ���Դ
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
        /// �رն����ߣ��ͷ���Դ
        /// </summary>
        internal override void ReleaseResource()
        {
            // �رն�����
            CloseConsumer();

            // �ͷ���Դ
            base.ReleaseResource();
        }

        /// <summary>
        /// �رն�����
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
        /// ��ȡ���⣬ͬ����ʽ
        /// </summary>
        /// <param name="aTopic">����</param>
        /// <param name="aTimeout">��ʱ��������0��ʾһֱ�ȴ�</param>
        /// <returns>���</returns>
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
        /// ��ȡ��Ϣ��ͬ����ʽ
        /// </summary>
        /// <param name="aTopic">����</param>
        /// <param name="aTimeout">��ʱ��������0��ʾһֱ�ȴ�</param>
        /// <param name="aSelector">ѡ����</param>
        /// <returns>MQ��Ϣ</returns>
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
        /// ע����Ϣ���������첽�ص�������ʽ
        /// </summary>
        /// <param name="aTopic">��������</param>
        /// <param name="aMessageListener">��Ϣ������</param>
        /// <param name="aSelector">ѡ����</param>
        internal void RegisterMessageListener(string aTopic, MessageListener aMessageListener, string aSelector)
        {
            //get topic
            TopicSub topicSub = GetTopic(aTopic, aSelector);

            // register message callback
            topicSub.RegisterMessageListener(aMessageListener);
        }

        /// <summary>
        /// ȡ��
        /// </summary>
        /// <param name="aTopic">����/����</param>
        /// <param name="aSelector">ѡ����</param>
        internal void UnConsume(string aTopic, string aSelector)
        {
            RemoveTopic(aTopic, aSelector);
        }

        // ��ȡ��Ϣ����
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
                            throw new Exception(String.Format("�������⣺{0}ʧ�ܣ�{1}", aTopic, ex.LinkedException.ToString()));
                        else
                            throw ex;
                    }
                }
            }
            
            return topicSub;
        }

        // �Ƴ����ر���Ϣ����
        private void RemoveTopic(string aTopic, string aSelector)
        {
            TopicSub topicSub = null;

            // �Ƴ�����
            lock (_topics)
            {
                string subscription = String.IsNullOrEmpty(aSelector) ? aTopic : aTopic + aSelector;
                if (_topics.ContainsKey(subscription))
                {
                    topicSub = _topics[subscription];
                    _topics.Remove(subscription);
                }
            }

            // �ر�����
            if (topicSub != null)
                topicSub.Close();
        }        


        /// <summary>
        /// ���ⶩ��
        /// </summary>
        class TopicSub
        {
            // ������
            private MQConsumer _mqConsumer;

            private string _topic;

            private string _selector;

            private IDestination _destination;

            private IMessageConsumer _consumer;           

            // ��Ϣ����
            private MessageListener _messageListener;


            /// <summary>
            /// �������ⶩ��
            /// </summary>
            /// <param name="topic">����</param>
            /// <param name="selector">��������</param>
            /// <param name="mqConsumer">������</param>
            public TopicSub(string topic, string selector, MQConsumer mqConsumer)
            {
                _topic = topic;
                _selector = selector;
                _mqConsumer = mqConsumer;
            }

            /// <summary>
            /// �������Ϣ
            /// </summary>
            public string Topic { get { return _topic; } }
            
            /// <summary>
            /// ������
            /// </summary>
            public string Selector { get { return _selector; } }
                        
            /// <summary>
            /// Ŀ�ĵص�ַ
            /// </summary>
            public IDestination Destination { get { return _destination; } }
            
            /// <summary>
            /// ������
            /// </summary>
            public IMessageConsumer Consumer { get { return _consumer; } }

            /// <summary>
            /// �����ߵı�ʶ
            /// </summary>
            public string Subscription { get { return _selector == null ? _topic : _topic + _selector; } }
            
            /// <summary>
            /// ע�ᶩ������
            /// </summary>
            public void Open()
            {
                // ����Ŀ�ĵ�
                if (_topic.StartsWith("topic://"))
                    _destination = _mqConsumer.Session.CreateTopic(_topic);
                else
                    _destination = _mqConsumer.Session.CreateQueue(_topic);
                _destination.SetIntProperty(XMSC.DELIVERY_MODE, XMSC.DELIVERY_PERSISTENT);

                // ����������
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
            /// �رն�������
            /// </summary>
            public void Close()
            {
                // �ȹر������ߣ����ܳɹ�ע������

                // �ر�������
                CloseConsumer();

                // ע������
                Unsubscribe();
            }

            // �ر�������
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
                    // ����ԭ�ȵ������ѶϿ����ɺ��Դ��쳣��
                }
                _consumer = null;
            }

            // ע������
            private void Unsubscribe()
            {
                try
                {
                    // ע�����ġ���MQ������ɾ���ѷ������Ϣ��ɾ�������ߡ�
                    if (_topic.StartsWith("topic://") && _mqConsumer.IsDurable)
                        _mqConsumer.Session.Unsubscribe(Subscription);
                }
                catch
                {
                    // ����ԭ�ȵ������ѶϿ����ɺ��Դ��쳣��
                }
            }

            /// <summary>
            /// ע����Ϣ������
            /// </summary>
            /// <param name="messageListener">��Ϣ������</param>
            public void RegisterMessageListener(MessageListener messageListener)
            {
                _messageListener = messageListener;
                if (_messageListener != null && Consumer != null)
                    Consumer.MessageListener = _messageListener;
            }
        }
    }



    /// <summary>
    /// ������
    /// </summary>
    public class MQProducer : MQSession
    {
        // ���淢����Ŀ�ĵ��ֵ�
        Dictionary<string, IDestination> _destinations = new Dictionary<string, IDestination>();

        // XMS�����߶���
        private IMessageProducer _producer;

        // ��Ϣ����ʱ��
        private long _timeToLive = 0;

        /// <summary>
        /// ���췢���ߣ�������Ϣ����ʱ��Ϊ0����ʾ��������
        /// </summary>
        protected MQProducer()
            : this(0)
        {
        }

        /// <summary>
        /// ���췢����
        /// </summary>
        protected MQProducer(long timeToLive)
        {
            _timeToLive = timeToLive;
        }

        /// <summary>
        /// �򿪷�����
        /// </summary>
        protected override void Open()
        {
            // �ȴ�Session
            base.Open();

            // ����XMS����
            _producer = Session.CreateProducer(null);
        }

        /// <summary>
        /// ���´򿪷�����
        /// </summary>
        internal override void ReOpen()
        {
            // �������Ŀ�ĵ���Ϣ������Session���£���ЩĿ�ĵؿ����Ѿ�ʧЧ��
            lock (_destinations)
            {
                _destinations.Clear();
            }

            this.Open();
        }

        /// <summary>
        /// �ͷ���Դ
        /// </summary>
        internal override void ReleaseResource()
        {
            // �رշ�����
            CloseProducer();

            // �ͷ�Session
            base.ReleaseResource();
        }

        /// <summary>
        /// �رշ�����
        /// </summary>
        private void CloseProducer()
        {
            try
            {
                // �������Ŀ�ĵ�
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
                // �ر�ʧ�ܣ���������������ԭ�򣬿ɺ��Դ��쳣
            }

            _producer = null;
        }

        /// <summary>
        /// ����������Ϣ
        /// </summary>
        /// <param name="topic">����</param>
        /// <param name="objGraph">����</param>
        protected void Produce(string topic, byte[] objGraph)
        {
            Produce(topic, objGraph, true);
        }

        /// <summary>
        /// ����������Ϣ
        /// </summary>
        /// <param name="topic">����</param>
        /// <param name="objGraph">����</param>
        /// <param name="retry">���Դ���</param>
        private void Produce(string topic, byte[] objGraph, bool retry)
        {
            try
            {
                // ���������ȡĿ�ĵ�
                IDestination dest = GetTopic(topic);

                // ������Ϣ
                IObjectMessage msg = Session.CreateObjectMessage();
                msg.SetObject(objGraph);

                // ������Ϣ
                _producer.Send(dest, msg, DeliveryMode.Persistent, _producer.Priority, _timeToLive);
            }
            catch (Exception ex)
            {
                if (retry)
                {
                    // �������ӣ�Ȼ���ٷ�����Ϣ
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
        /// ����������Ϣ
        /// </summary>
        /// <param name="topic">����</param>
        /// <param name="aMessage">MQ��Ϣ</param>
        protected void Produce(string topic, MQMessage aMessage)
        {
            Produce(topic, aMessage, true);
        }

        /// <summary>
        /// ����������Ϣ
        /// </summary>
        /// <param name="topic">����</param>
        /// <param name="aMessage">MQ��Ϣ</param>
        /// <param name="retry">���Դ���</param>
        private void Produce(string topic, MQMessage aMessage, bool retry)
        {
            try
            {
                string s = string.Format("aMessage={0}", aMessage);
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Debug, s);
                // ���������ȡĿ�ĵ�
                IDestination dest = GetTopic(topic);

                // ������Ϣ
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

                // ת����Ϣͷ
                if (msg != null)
                {
                    aMessage.ToMessage(msg);
                }
                s = string.Format("dest={0},msg={1},_producer.Priority={2},_timeToLive={3}"
                    , dest, msg, _producer.Priority, _timeToLive);
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Debug, s);
                // ������Ϣ
                _producer.Send(dest, msg, DeliveryMode.Persistent, _producer.Priority, _timeToLive);
            }
            catch (Exception ex)
            {

                string s = string.Format("topic={0},aMessage={1},retry={2},IDestination={3},_producer={4},Session={5}\r\nexception={6}"
                    , topic, aMessage, retry, GetTopic(topic), _producer, Session,ex.Message);
                xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, s);
                if (retry)
                {
                    // �������ӣ�Ȼ���ٷ�����Ϣ
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
        /// �ӻ����л�ȡĿ�ĵأ������û�б����棬�򴴽��µĶ��󲢻�������
        /// </summary>
        /// <param name="topic">����</param>
        /// <returns>���������Ӧ��IDestination����</returns>
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
        /// �������ⴴ��IDestination����
        /// </summary>
        /// <param name="topic">����</param>
        /// <returns>���ش�����IDestination����</returns>
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
