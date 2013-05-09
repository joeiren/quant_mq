/*********************************************************************
 * ���ƣ� MQConnection.cs
 * ���ܣ� MQ����
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.12
 * �汾�� 1.2.1
 * �޶���
 *        1.0.0 --- ֧��MQ����
 *        1.2.0 --- �κ����ع����� By 2009-04-10
 *        1.2.1 --- �κ��ܣ���������ʱ����Ϣ���ⱻ��ס���²���������������ʱ����������ʧ���¼�
 *********************************************************************/

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// MQ���Ӳ�����
    /// </summary>
    public sealed class MQParameters
    {
        /// <summary>
        /// ���й�������������������IP��ַҲ�����ǻ�������
        /// </summary>
        public MQParameter<string> QmgrHostname { get { return _qmgrHostName; } }

        /// <summary>
        /// ���й����������˿ڣ�Ĭ��Ϊ1414
        /// </summary>
        public MQParameter<int> QmgrPort { get { return _qmgrPort; } }

        /// <summary>
        /// ���й���������
        /// </summary>
        public MQParameter<string> QmgrName { get { return _qmgrName; } }

        /// <summary>
        /// ��Ϣ����ģʽ��Ĭ��Ϊ�ͻ���ģʽ
        ///     XMSC.WMQ_CM_BINDINGS = 0����ģʽ�������ڱ������ӣ�
        ///     XMSC.WMQ_CM_CLIENT = 1���ͻ���ģʽ����Զ������
        /// </summary>
        public MQParameter<int> ConnectionMode { get { return _connectionMode; } }

        /// <summary>
        /// ����汾��Ĭ��ΪWMQ_BROKER_V1
        /// </summary>
        public MQParameter<int> BrokerVersion { get { return _brokerVersion; } }

        /// <summary>
        /// �ͻ��˱�ʶId
        /// </summary>
        public MQParameter<string> ClientId { get { return _clientId; } }

        private MQParameter<string> _qmgrHostName = new MQParameter<string>(XMSC.WMQ_HOST_NAME);
        private MQParameter<int> _qmgrPort = new MQParameter<int>(XMSC.WMQ_PORT, XMSC.WMQ_DEFAULT_CLIENT_PORT);
        private MQParameter<string> _qmgrName = new MQParameter<string>(XMSC.WMQ_QUEUE_MANAGER);
        private MQParameter<int> _connectionMode = new MQParameter<int>(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
        private MQParameter<int> _brokerVersion = new MQParameter<int>(XMSC.WMQ_BROKER_VERSION, XMSC.WMQ_BROKER_V1);
        private MQParameter<string> _clientId = new MQParameter<string>(XMSC.CLIENT_ID);
    }


    /// <summary>
    /// ʧ��ԭ��
    /// </summary>
    public enum FailReasion
    {
        /// <summary>
        /// �������ӳ�ʱ��ȱʡ��������ʱ��Ϊ30����
        /// </summary>
        ReConnectTimeout,

        /// <summary>
        /// ���ⱻ��ס����ʱֻ��������������ſ��Խ���
        /// </summary>
        TopicLocked
    }

    /// <summary>
    /// ʧ��ԭ��
    /// </summary>
    public class ConnectionFailedEventArgs : EventArgs
    {
        private readonly FailReasion _reasion;

        /// <summary>
        /// ���캯��
        /// </summary>
        /// <param name="reasion">ʧ��ԭ��</param>
        public ConnectionFailedEventArgs(FailReasion reasion)
        {
            _reasion = reasion;
        }

        /// <summary>
        /// ʧ��ԭ��
        /// </summary>
        public FailReasion Reasion {get { return _reasion; } }
    }

    /// <summary>
    /// ����ʧ��ί��
    /// </summary>
    /// <param name="sender">���Ͷ���</param>
    /// <param name="e">�¼�</param>
    public delegate void ConnectionFailedHandler(object sender, ConnectionFailedEventArgs e);


    /// <summary>
    /// �ṩMQ���ӵĳ�ʼ�������ӣ������ȹ���
    /// </summary>
    public sealed class MQConnection
    {
        // �ڲ���
        private static object _locker = new object();

        // ȱʡ���Ӷ���
        private static MQConnection _default;
        
        // ���ӹ���
        private IConnectionFactory _connFactory;

        // ���Ӳ���
        private MQParameters _parameters = new MQParameters();

        // ����״̬
        private bool _connected = false;

        // �Ƿ�ر���
        private bool _closing = false;

        // XMS�����Ӷ���
        private IConnection _raw;

        // Session�б�
        private Dictionary<string, MQSession> _mqSessions = new Dictionary<string, MQSession>();

        // ��ʱ�����̣߳������ӶϿ�ʱ��������ʱ�����߳��Զ�����MQ
        private Thread _thread = null;
        private AutoResetEvent _event = null;
        

        /// <summary>
        /// ���ӱ仯�¼�
        /// </summary>
        public event EventHandler ConnectionChanged;

        /// <summary>
        /// ����ʧ���¼�
        /// </summary>
        public event ConnectionFailedHandler ConnectionFailed;
        
        /// <summary>
        /// ���캯��
        /// </summary>
        public MQConnection()
        {
        }
                
        /// <summary>
        /// Ĭ�����ӣ�һ�������һ��MQ�ͻ���ֻ��һ������
        /// </summary>
        public static MQConnection Default
        {
            get
            {
                if (_default == null)
                {
                    lock (_locker)
                    {
                        if (_default == null)
                            _default = new MQConnection();
                    }
                }
                return _default;
            }
        }


        /// <summary>
        /// ���Ӳ���
        /// </summary>
        public MQParameters Parameters { get { return _parameters; } }

        /// <summary>
        /// ����״̬
        /// </summary>
        public bool Connected { get { return _connected; } }

        /// <summary>
        /// ����XMS��Session����
        /// </summary>
        /// <returns>����Session����</returns>
        internal ISession CreateSession()
        {
            if (_raw != null)
                return _raw.CreateSession(false, AcknowledgeMode.AutoAcknowledge);
            return null;
        }

        /// <summary>
        /// ����MQSession
        /// </summary>
        /// <param name="mqSession">MQSession����</param>
        internal void AddSession(MQSession mqSession)
        {
            lock (_mqSessions)
            {
                if (!_mqSessions.ContainsKey(mqSession.SessionName))           
                    _mqSessions.Add(mqSession.SessionName, mqSession);
            }
        }

        /// <summary>
        /// �Ƴ�Session
        /// </summary>
        /// <param name="mqSession">MQSession����</param>
        internal void RemoveSession(MQSession mqSession)
        {
            lock (_mqSessions)
            {
                _mqSessions.Remove(mqSession.SessionName);
            }
        }


        /// <summary>
        /// ��ʼ�����Ӳ���
        /// </summary>
        /// <remarks>������ʼ���󲢲��������ӣ����ֶ�����Open������������</remarks>
        public void Init(string aClientId, string aQmgrHostName, int aQmgrPort, string aQmgrName)
        {
            if (string.IsNullOrEmpty(aQmgrHostName) || string.IsNullOrEmpty(aQmgrName))
                throw new Exception("MQ���Ӳ�������Ϊ�ա�");

            _parameters.QmgrHostname.Value = aQmgrHostName;
            _parameters.QmgrPort.Value = aQmgrPort;
            _parameters.QmgrName.Value = aQmgrName;
            _parameters.ClientId.Value = aClientId;

            // create the connection factories factory
            XMSFactoryFactory factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);

            // create a connection factory
            _connFactory = factoryFactory.CreateConnectionFactory();

            // set the properties
            _connFactory.SetStringProperty(_parameters.QmgrHostname.Name, _parameters.QmgrHostname.Value);
            _connFactory.SetIntProperty(_parameters.QmgrPort.Name, _parameters.QmgrPort.Value);
            _connFactory.SetStringProperty(_parameters.QmgrName.Name, _parameters.QmgrName.Value);
            _connFactory.SetIntProperty(_parameters.ConnectionMode.Name, _parameters.ConnectionMode.Value);
            _connFactory.SetIntProperty(_parameters.BrokerVersion.Name, _parameters.BrokerVersion.Value);
            _connFactory.SetStringProperty(_parameters.ClientId.Name, _parameters.ClientId.Value);
        }

        /// <summary>
        /// �������ӡ�δ�������������ܽ�����Ϣ�����ǲ��ܷ�����Ϣ��
        /// </summary>
        public void Start()
        {
            if (_raw != null)
                _raw.Start();
        }

        /// <summary>
        /// ֹͣ���ӡ�����ֹͣ�����ܽ�����Ϣ�����ǲ��ܷ�����Ϣ��
        /// </summary>
        public void Stop()
        {
            try
            {
                if (_raw != null)
                    _raw.Stop();
            }
            catch
            {
                // ���Ը��쳣����
            }
        }

        /// <summary>
        /// ����MQ����
        /// </summary>
        public void Open()
        {
            _closing = false;
            Open(true);
        }

        
        /// <summary>
        /// ����MQ����
        /// </summary>
        /// <param name="needLog">�Ƿ���Ҫ��¼Log</param>
        private void Open(bool needLog)
        {
            // ��������ӣ���û�б�Ҫ�����½�������
            if (_connected)
                return;

            if (_connFactory == null)
                throw new Exception("MQ���Ӳ���δ��ʼ�������ȵ���Init�������г�ʼ����");

            IConnection _rawOld = _raw;
            try
            {
                // �Ƴ��쳣������
                if (_raw != null)
                    _raw.ExceptionListener = null;

                // �������ӣ����ӽ����󼴿ɷ�����Ϣ�������ܽ�����Ϣ��
                _raw = _connFactory.CreateConnection();

                // �����ӣ����Ӵ򿪺󼴿ɷ�����Ϣ�ͽ�����Ϣ��
                _raw.Start();

                // �����쳣������
                _raw.ExceptionListener = new ExceptionListener(DoMQException);

                _connected = true;

                if (ConnectionChanged != null)
                    ConnectionChanged(this, EventArgs.Empty);
            }
            catch (Exception ex)
            {
                _connected = false;
                if (needLog)
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, string.Format("{0}.Open�쳣:\n{1}", GetType().FullName, ex.ToString()));
                throw ex;
            }

            // ������ӳɹ�����Ҫ���ϵ����ӹرգ����ͷ���Դ
            CloseConnection(_rawOld);
        }

        /// <summary>
        /// �ر�MQ���ӣ��ͷ���Դ
        /// </summary>
        public void Close()
        {
            _closing = true;

            // ֹͣ�Զ������߳�
            if (_event != null)
            {
                _event.Set();
            }

            // ��ֹͣ���ӣ���ֹ�䷢����Ϣ������ע�����ĺ͹ر����Ӻ�ʱ��Ƚ���
            Stop();

            // ֪ͨ��ע���MQSessionע���Լ��ķ����ߺͶ�����
            List<MQSession> lstSession = new List<MQSession>();
            lock (_mqSessions)
            {
                lstSession.AddRange(_mqSessions.Values);
                _mqSessions.Clear();
            }

            // �ر�Session
            foreach (MQSession mqSession in lstSession)
            {
                mqSession.Close();
            }

            // �ر�Connection
            CloseConnection(_raw);

            // ��������״̬
            _raw = null;
            _connected = false;

            if (ConnectionChanged != null)
                ConnectionChanged(this, EventArgs.Empty);
        }


        /// <summary>
        /// ���´����ӣ������ǰ��״̬�������ϣ��򲻽�������
        /// </summary>
        public void ReOpen()
        {
            if (_thread == null)
            {
                lock (this)
                {
                    if (_thread == null)
                    {
                        _connected = false;
                        ReOpenCore();
                    }
                }
            }
        }

        /// <summary>
        /// ���´����ӣ������ǰ��״̬�������ϣ��򲻽�������
        /// </summary>
        private void ReOpenCore()
        {
            lock (this)
            {
                // ��������ӳɹ�����ֱ���˳�����Ϊ�÷������ܱ����̵߳��ã�������һ���߳�����ø÷������ӳɹ����������߳�ֱ���˳�
                if (_connected) return;

                try
                {
                    // �����ӡ���������쳣���ɺ��ԣ����ս��������ӵ�״̬��
                    Open(false);
                }
                catch (Exception ex)
                {
                    // ��д��־�ˣ�������ܶ����־
                    // xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, string.Format("{0}.ReOpen�쳣:\n{1}", GetType().FullName, ex.ToString()));
                }

                if (_connected)
                {
                    if (_event != null)
                        _event.Set();

                    // ֪ͨ��ע���MQSession����ע���Լ��ķ����ߺͶ�����
                    foreach (KeyValuePair<string, MQSession> entry in _mqSessions)
                    {
                        try
                        {
                            entry.Value.ReOpen();
                        }
                        catch(Exception exp)
                        {
                            // ����ֻд��־�����������쳣��������һ��Session�Ĳ�����
                            xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, "MQ Session reopen failed:" + exp.Message + "\r\n");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// �ر�����
        /// </summary>
        private void CloseConnection(IConnection connection)
        {
            try
            {
                if (connection != null)
                {
                    connection.Close();
                    connection.Dispose();
                    GC.Collect();
                }
            }
            catch
            {
                // ���Թر�֮ǰ�����ӡ����֮ǰ�������Ѿ��Ͽ������������쳣���ɺ��ԡ�
            }
        }
        
        /// <summary>
        /// MQ�����쳣�������Զ������߳�
        /// </summary>
        /// <param name="ex">�쳣��Ϣ</param>
        private void DoMQException(Exception ex)
        {
            if (_thread != null)
                return;

            xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, String.Format("������MQ�����쳣�����Զ��������쳣��Ϣ:\n{0}", ex));

            lock (this)
            {
                if (!_closing && (_thread == null))
                {
                    _connected = false;

                    _raw.ExceptionListener = null;

                    // ��ֹͣ���ӣ���ֹ�䷢����Ϣ
                    Stop();

                    // �������ӱ仯�¼�
                    if (ConnectionChanged != null)
                        ConnectionChanged(this, EventArgs.Empty);

                    // ������ʱ�Զ�����
                    _event = new AutoResetEvent(false);
                    _thread = new Thread(new ThreadStart(TimerReOpenExectue));
                    _thread.Name = "MQ��ʱ�����߳�";
                    _thread.Start();
                }
            }
        }

        /// <summary>
        /// ��������ʧ���¼�
        /// </summary>
        /// <param name="reasion">����ʧ��ԭ��</param>
        internal void RaiseConnectionFailed(FailReasion reasion)
        {
            if (ConnectionFailed != null)
                ConnectionFailed(this, new ConnectionFailedEventArgs(reasion));
        }

        /// <summary>
        /// ��ʱ�����߳�ִ�к���
        /// </summary>
        private void TimerReOpenExectue()
        {
            // �ر�Session��ע�����ⶩ��
            List<MQSession> lstSession = new List<MQSession>();
            lock (_mqSessions)
            {
                lstSession.AddRange(_mqSessions.Values);
            }
            foreach (MQSession mqSession in lstSession)
            {
                mqSession.ReleaseResource();
            }

            // �ر�����
            CloseConnection(_raw);
            _raw = null;

            long elapseTime = 0;
            while (true)
            {
                bool bEventSignal = _event.WaitOne(2000, false);

                // ����ȵ��źţ�������߳�ִ��
                if (bEventSignal)
                {
                    _event.Close();
                    _event = null;
                    _thread = null;
                    break;
                }
                else
                {
                    if (ConnectionFailed != null)
                    {
                        elapseTime += 2000;
                        if (elapseTime >= MQConst.MsgTimeToLive)
                        {
                            RaiseConnectionFailed(FailReasion.ReConnectTimeout);
                            return;
                        }
                    }

                    // û�еȵ��źţ�������MQ
                    ReOpenCore();
                }
            }
        }


    }
}
