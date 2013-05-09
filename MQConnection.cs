/*********************************************************************
 * 名称： MQConnection.cs
 * 功能： MQ连接
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.12
 * 版本： 1.2.1
 * 修订：
 *        1.0.0 --- 支持MQ连接
 *        1.2.0 --- 廖洪周重构代码 By 2009-04-10
 *        1.2.1 --- 廖洪周，增加重连时，消息主题被锁住导致不能重新连接主题时，触发连接失败事件
 *********************************************************************/

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// MQ连接参数类
    /// </summary>
    public sealed class MQParameters
    {
        /// <summary>
        /// 队列管理器主机名，可以是IP地址也可以是机器名字
        /// </summary>
        public MQParameter<string> QmgrHostname { get { return _qmgrHostName; } }

        /// <summary>
        /// 队列管理器侦听端口，默认为1414
        /// </summary>
        public MQParameter<int> QmgrPort { get { return _qmgrPort; } }

        /// <summary>
        /// 队列管理器名称
        /// </summary>
        public MQParameter<string> QmgrName { get { return _qmgrName; } }

        /// <summary>
        /// 消息连接模式，默认为客户端模式
        ///     XMSC.WMQ_CM_BINDINGS = 0，绑定模式，仅限于本机连接；
        ///     XMSC.WMQ_CM_CLIENT = 1，客户端模式，可远程连接
        /// </summary>
        public MQParameter<int> ConnectionMode { get { return _connectionMode; } }

        /// <summary>
        /// 代理版本，默认为WMQ_BROKER_V1
        /// </summary>
        public MQParameter<int> BrokerVersion { get { return _brokerVersion; } }

        /// <summary>
        /// 客户端标识Id
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
    /// 失败原因
    /// </summary>
    public enum FailReasion
    {
        /// <summary>
        /// 重新连接超时，缺省重新连接时间为30分钟
        /// </summary>
        ReConnectTimeout,

        /// <summary>
        /// 主题被锁住，此时只能重新启动程序才可以解锁
        /// </summary>
        TopicLocked
    }

    /// <summary>
    /// 失败原因
    /// </summary>
    public class ConnectionFailedEventArgs : EventArgs
    {
        private readonly FailReasion _reasion;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="reasion">失败原因</param>
        public ConnectionFailedEventArgs(FailReasion reasion)
        {
            _reasion = reasion;
        }

        /// <summary>
        /// 失败原因
        /// </summary>
        public FailReasion Reasion {get { return _reasion; } }
    }

    /// <summary>
    /// 连接失败委托
    /// </summary>
    /// <param name="sender">发送对象</param>
    /// <param name="e">事件</param>
    public delegate void ConnectionFailedHandler(object sender, ConnectionFailedEventArgs e);


    /// <summary>
    /// 提供MQ连接的初始化，连接，重连等功能
    /// </summary>
    public sealed class MQConnection
    {
        // 内部锁
        private static object _locker = new object();

        // 缺省连接对象
        private static MQConnection _default;
        
        // 连接工厂
        private IConnectionFactory _connFactory;

        // 连接参数
        private MQParameters _parameters = new MQParameters();

        // 连接状态
        private bool _connected = false;

        // 是否关闭中
        private bool _closing = false;

        // XMS的连接对象
        private IConnection _raw;

        // Session列表
        private Dictionary<string, MQSession> _mqSessions = new Dictionary<string, MQSession>();

        // 定时重连线程，当连接断开时，启动定时重连线程自动重连MQ
        private Thread _thread = null;
        private AutoResetEvent _event = null;
        

        /// <summary>
        /// 连接变化事件
        /// </summary>
        public event EventHandler ConnectionChanged;

        /// <summary>
        /// 连接失败事件
        /// </summary>
        public event ConnectionFailedHandler ConnectionFailed;
        
        /// <summary>
        /// 构造函数
        /// </summary>
        public MQConnection()
        {
        }
                
        /// <summary>
        /// 默认连接，一般情况下一个MQ客户端只开一个连接
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
        /// 连接参数
        /// </summary>
        public MQParameters Parameters { get { return _parameters; } }

        /// <summary>
        /// 连接状态
        /// </summary>
        public bool Connected { get { return _connected; } }

        /// <summary>
        /// 创建XMS的Session对象
        /// </summary>
        /// <returns>返回Session对象</returns>
        internal ISession CreateSession()
        {
            if (_raw != null)
                return _raw.CreateSession(false, AcknowledgeMode.AutoAcknowledge);
            return null;
        }

        /// <summary>
        /// 增加MQSession
        /// </summary>
        /// <param name="mqSession">MQSession对象</param>
        internal void AddSession(MQSession mqSession)
        {
            lock (_mqSessions)
            {
                if (!_mqSessions.ContainsKey(mqSession.SessionName))           
                    _mqSessions.Add(mqSession.SessionName, mqSession);
            }
        }

        /// <summary>
        /// 移除Session
        /// </summary>
        /// <param name="mqSession">MQSession对象</param>
        internal void RemoveSession(MQSession mqSession)
        {
            lock (_mqSessions)
            {
                _mqSessions.Remove(mqSession.SessionName);
            }
        }


        /// <summary>
        /// 初始化连接参数
        /// </summary>
        /// <remarks>参数初始化后并不创建连接，需手动调用Open方法创建连接</remarks>
        public void Init(string aClientId, string aQmgrHostName, int aQmgrPort, string aQmgrName)
        {
            if (string.IsNullOrEmpty(aQmgrHostName) || string.IsNullOrEmpty(aQmgrName))
                throw new Exception("MQ连接参数不能为空。");

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
        /// 启动连接。未启动的连接仍能接收消息，但是不能发送消息。
        /// </summary>
        public void Start()
        {
            if (_raw != null)
                _raw.Start();
        }

        /// <summary>
        /// 停止连接。连接停止后仍能接收消息，但是不能发送消息。
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
                // 忽略该异常处理
            }
        }

        /// <summary>
        /// 建立MQ连接
        /// </summary>
        public void Open()
        {
            _closing = false;
            Open(true);
        }

        
        /// <summary>
        /// 建立MQ连接
        /// </summary>
        /// <param name="needLog">是否需要记录Log</param>
        private void Open(bool needLog)
        {
            // 如果已连接，则没有必要再重新建立连接
            if (_connected)
                return;

            if (_connFactory == null)
                throw new Exception("MQ连接参数未初始化，请先调用Init方法进行初始化。");

            IConnection _rawOld = _raw;
            try
            {
                // 移除异常侦听器
                if (_raw != null)
                    _raw.ExceptionListener = null;

                // 建立连接，连接建立后即可发送消息，但不能接收消息。
                _raw = _connFactory.CreateConnection();

                // 打开连接，连接打开后即可发送消息和接收消息。
                _raw.Start();

                // 设置异常侦听器
                _raw.ExceptionListener = new ExceptionListener(DoMQException);

                _connected = true;

                if (ConnectionChanged != null)
                    ConnectionChanged(this, EventArgs.Empty);
            }
            catch (Exception ex)
            {
                _connected = false;
                if (needLog)
                    xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, string.Format("{0}.Open异常:\n{1}", GetType().FullName, ex.ToString()));
                throw ex;
            }

            // 如果连接成功，需要把老的连接关闭，以释放资源
            CloseConnection(_rawOld);
        }

        /// <summary>
        /// 关闭MQ连接，释放资源
        /// </summary>
        public void Close()
        {
            _closing = true;

            // 停止自动重连线程
            if (_event != null)
            {
                _event.Set();
            }

            // 先停止连接，阻止其发送消息。这样注销订阅和关闭连接耗时会比较少
            Stop();

            // 通知已注册的MQSession注销自己的发布者和订阅者
            List<MQSession> lstSession = new List<MQSession>();
            lock (_mqSessions)
            {
                lstSession.AddRange(_mqSessions.Values);
                _mqSessions.Clear();
            }

            // 关闭Session
            foreach (MQSession mqSession in lstSession)
            {
                mqSession.Close();
            }

            // 关闭Connection
            CloseConnection(_raw);

            // 设置连接状态
            _raw = null;
            _connected = false;

            if (ConnectionChanged != null)
                ConnectionChanged(this, EventArgs.Empty);
        }


        /// <summary>
        /// 重新打开连接，如果当前的状态是已连上，则不进行重连
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
        /// 重新打开连接，如果当前的状态是已连上，则不进行重连
        /// </summary>
        private void ReOpenCore()
        {
            lock (this)
            {
                // 如果已连接成功，则直接退出，因为该方法可能被多线程调用，在其中一个线程里调用该方法连接成功，则其他线程直接退出
                if (_connected) return;

                try
                {
                    // 打开连接。如果产生异常，可忽略，最终将返回连接的状态。
                    Open(false);
                }
                catch (Exception ex)
                {
                    // 不写日志了，会产生很多该日志
                    // xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, string.Format("{0}.ReOpen异常:\n{1}", GetType().FullName, ex.ToString()));
                }

                if (_connected)
                {
                    if (_event != null)
                        _event.Set();

                    // 通知已注册的MQSession重新注册自己的发布者和订阅者
                    foreach (KeyValuePair<string, MQSession> entry in _mqSessions)
                    {
                        try
                        {
                            entry.Value.ReOpen();
                        }
                        catch(Exception exp)
                        {
                            // 这里只写日志，不向外抛异常，继续下一个Session的操作。
                            xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, "MQ Session reopen failed:" + exp.Message + "\r\n");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 关闭连接
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
                // 尝试关闭之前的连接。如果之前的连接已经断开，这里会产生异常，可忽略。
            }
        }
        
        /// <summary>
        /// MQ连接异常，启动自动重连线程
        /// </summary>
        /// <param name="ex">异常信息</param>
        private void DoMQException(Exception ex)
        {
            if (_thread != null)
                return;

            xQuant.Log4.LogHelper.Write(xQuant.Log4.LogLevel.Error, String.Format("侦听到MQ连接异常，将自动重连！异常信息:\n{0}", ex));

            lock (this)
            {
                if (!_closing && (_thread == null))
                {
                    _connected = false;

                    _raw.ExceptionListener = null;

                    // 先停止连接，阻止其发送消息
                    Stop();

                    // 触发连接变化事件
                    if (ConnectionChanged != null)
                        ConnectionChanged(this, EventArgs.Empty);

                    // 启动定时自动连接
                    _event = new AutoResetEvent(false);
                    _thread = new Thread(new ThreadStart(TimerReOpenExectue));
                    _thread.Name = "MQ定时重连线程";
                    _thread.Start();
                }
            }
        }

        /// <summary>
        /// 触发连接失败事件
        /// </summary>
        /// <param name="reasion">连接失败原因</param>
        internal void RaiseConnectionFailed(FailReasion reasion)
        {
            if (ConnectionFailed != null)
                ConnectionFailed(this, new ConnectionFailedEventArgs(reasion));
        }

        /// <summary>
        /// 定时重连线程执行函数
        /// </summary>
        private void TimerReOpenExectue()
        {
            // 关闭Session，注销主题订阅
            List<MQSession> lstSession = new List<MQSession>();
            lock (_mqSessions)
            {
                lstSession.AddRange(_mqSessions.Values);
            }
            foreach (MQSession mqSession in lstSession)
            {
                mqSession.ReleaseResource();
            }

            // 关闭连接
            CloseConnection(_raw);
            _raw = null;

            long elapseTime = 0;
            while (true)
            {
                bool bEventSignal = _event.WaitOne(2000, false);

                // 如果等到信号，则结束线程执行
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

                    // 没有等到信号，则重连MQ
                    ReOpenCore();
                }
            }
        }


    }
}
