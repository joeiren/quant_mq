/*********************************************************************
 * ���ƣ� MQReceiver.cs
 * ���ܣ� ��Ϣ������
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.21
 * ���£� 
 * �汾�� 1.1.0
 * �޶���
 *        1.0.0 --- ��ָ������Ϣ����ͬ��/�첽������Ϣ
 *        1.1.0 --- �κ����ع����룬By 2009-04-11
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// ��Ϣ������
    /// </summary>
    public class MQReceiver : MQConsumer
    {
        // �ص��ӿ�
        private MessageReceiveCallBack _messageReceiveCallBack;
        
        // ��������
        private string _queueName;


        /// <summary>
        /// ������Ϣ������
        /// </summary>
        public MQReceiver()
            : base()
        {
        }


        /// <summary>
        /// ��ʼ�������ߣ������ʼ��ʧ�ܣ����׳��쳣
        /// </summary>
        /// <param name="aQueueName">��������</param>
        public void Init(string aQueueName)
        {
            _queueName = String.Format("queue://{0}", aQueueName);
            base.InitCore();
        }

        /// <summary>
        /// ��ʼ�������ߣ������ʼ��ʧ�ܣ����׳��쳣
        /// </summary>
        /// <param name="aQueueName">��������</param>
        /// <param name="mqConnection">MQ����</param>
        public void Init(string aQueueName, MQConnection mqConnection)
        {
            _queueName = String.Format("queue://{0}", aQueueName);
            base.InitCore(mqConnection);
        }

        /// <summary>
        /// ͬ��������Ϣ
        /// </summary>
        /// <param name="aTimeout">��ʱ��������0��ʾһֱ�ȴ�</param>
        /// <returns>MQ��Ϣ</returns>
        public MQMessage ReceiveMessage(int aTimeout)
        {
            return ReceiveMessage(aTimeout, String.Empty);
        }

        /// <summary>
        /// ͬ��������Ϣ
        /// </summary>
        /// <param name="aTimeout">��ʱ��������0��ʾһֱ�ȴ�</param>
        /// <param name="aSelector">ѡ����</param>
        /// <returns>MQ��Ϣ</returns>
        public MQMessage ReceiveMessage(int aTimeout, string aSelector)
        {
            return Consume(_queueName, aTimeout, aSelector);
        }

        /// <summary>
        /// �첽������Ϣ
        /// </summary>
        /// <param name="aMessageReceiveCallBack">��Ϣ���ջص�����</param>
        public void RegisterMessageReceiver(MessageReceiveCallBack aMessageReceiveCallBack)
        {
             RegisterMessageReceiver(aMessageReceiveCallBack, string.Empty);
            //_messageReceiveCallBack = aMessageReceiveCallBack;
            //RegisterMessageListener(_queueName, this.OnMessageListener, String.Empty);
        }

        /// <summary>
        /// �첽������Ϣ
        /// </summary>
        /// <param name="aMessageReceiveCallBack">��Ϣ���ջص�����</param>
        public void RegisterMessageReceiver(MessageReceiveCallBack aMessageReceiveCallBack, string aSelector)
        {
            _messageReceiveCallBack = aMessageReceiveCallBack;
            RegisterMessageListener(_queueName, this.OnMessageListener, aSelector);
        }

        /// <summary>
        /// �Ӷ����ж�����Ϣ�����ر�consume����
        /// </summary>
        /// <param name="aTimeout">��ʱ��������0��ʾһֱ�ȴ�</param>
        /// <param name="aSelector">ѡ����</param>
        /// <returns>MQ��Ϣ</returns>
        public MQMessage ReceiveAndRemoveMessage(int aTimeout, string aSelector)
        {
            MQMessage msg = Consume(_queueName, aTimeout, aSelector);
            UnConsume(_queueName, aSelector);
            return msg;
        }

        /// <summary>
        /// �����Ѷ�������Ļص�����
        /// </summary>
        /// <param name="msg">�Ѷ��ĵ���Ϣ</param>
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
