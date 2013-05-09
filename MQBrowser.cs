/*********************************************************************
 * ���ƣ� MQBrowser.cs
 * ���ܣ� ��Ϣ�����
 * ���ߣ� �κ���
 * ��˾�� xQuant
 * ���ڣ� 2009.04.14
 * ���£� 
 * �汾�� 1.0.0
 * �޶���
 *        1.0.0 --- ��������
 *********************************************************************/

using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// �����
    /// </summary>
    public class MQBrowser : MQSession
    {
        // ��������
        private string _queueName = null;

        // XMS������߶���
        private IQueueBrowser _browser;

        // Ŀ�ĵ�
        private IDestination _destination;

        /// <summary>
        /// ���������
        /// </summary>
        public MQBrowser()
        {
        }

        /// <summary>
        /// ��ʼ������ߣ������ʼ��ʧ�ܣ����׳��쳣
        /// </summary>
        /// <param name="aQueueName">��������</param>
        public void Init(string aQueueName)
        {
            _queueName = String.Format("queue:///{0}", aQueueName);
            base.InitCore();
        }

        /// <summary>
        /// ��ʼ������ߣ������ʼ��ʧ�ܣ����׳��쳣
        /// </summary>
        /// <param name="aQueueName">��������</param>
        /// <param name="mqConnection">MQ����</param>
        public void Init(string aQueueName, MQConnection mqConnection)
        {
            _queueName = String.Format("queue:///{0}", aQueueName);
            base.InitCore(mqConnection);
        }

        /// <summary>
        /// �������
        /// </summary>
        protected override void Open()
        {
            // �ȴ�Session
            base.Open();

            // ����Ŀ�ĵ�
            _destination = Session.CreateQueue(_queueName);
            _destination.SetIntProperty(XMSC.DELIVERY_MODE, XMSC.DELIVERY_PERSISTENT);

            // ���������
            _browser = Session.CreateBrowser(_destination);
        }

        /// <summary>
        /// ���´������
        /// </summary>
        internal override void ReOpen()
        {
            this.Open();
        }

        /// <summary>
        /// �ر�����ߣ��ͷ���Դ
        /// </summary>
        internal override void ReleaseResource()
        {
            // �ر������
            CloseBrowser();

            // �ͷ���Դ
            base.ReleaseResource();
        }

        /// <summary>
        /// �رն�����
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
                // ����ԭ�ȵ������ѶϿ����ɺ��Դ��쳣��
            }

            _browser = null;
        }


        /// <summary>
        /// ͬ�������Ϣ
        /// </summary>
        /// <returns>������Ϣ������</returns>
        public IEnumerator BrowserMessage()
        {
            return _browser.GetEnumerator();
        }
    }
}
