/*********************************************************************
 * ���ƣ� MQSender.cs
 * ���ܣ� ��Ϣ������
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.21
 * ���£� 
 * �汾�� 1.1.0
 * �޶���
 *        1.0.0 --- ��ָ������Ϣ���з�����Ϣ
 *        1.1.0 --- �κ����ع����룬By 2009-04-11
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace xQuant.MQ
{
    /// <summary>
    /// ������
    /// </summary>
    public class MQSender : MQProducer
    {
        // ��������
        private string _queueName = null;

        /// <summary>
        /// ���췢����
        /// </summary>
        public MQSender()
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
        /// ������Ϣ������ʧ�ܣ����׳��쳣
        /// </summary>
        /// <param name="aMessage">MQ��Ϣ</param>
        public void SendMessage(MQMessage aMessage)
        {
            try
            {
                Produce(_queueName, aMessage);
            }
            catch(Exception exp)
            {
                Log4.LogHelper.Write(Log4.LogLevel.Error, exp.ToString());
                throw new Exception("����MQ��Ϣʧ�ܣ�������������������MQ���������У����Ժ����ԡ�");
            }
        }
    }
}
