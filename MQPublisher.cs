/*********************************************************************
 * ���ƣ� MQPublisher.cs
 * ���ܣ� ���ⷢ����
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.21
 * ���£� 
 * �汾�� 1.1.0
 * �޶���
 *        1.0.0 --- ��������
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
    public class MQPublisher : MQProducer
    {
        private BinaryFormatter _formatter = new BinaryFormatter();
        private MemoryStream _stream = new MemoryStream();

        /// <summary>
        /// ���췢����
        /// </summary>
        public MQPublisher()
            : base(MQConst.MsgTimeToLive)
        {
        }

        /// <summary>
        /// ��ʼ�������ߣ������ʼ��ʧ�ܣ����׳��쳣
        /// </summary>
        public void Init()
        {
            base.InitCore();
        }

        /// <summary>
        /// ��ʼ�������ߣ������ʼ��ʧ�ܣ����׳��쳣
        /// </summary>
        /// <param name="mqConnection">MQ����</param>
        public void Init(MQConnection mqConnection)
        {
            base.InitCore(mqConnection);
        }


        /// <summary>
        /// �ύ��������
        /// </summary>
        /// <param name="aTopic">����</param>
        /// <param name="aObj">�������б�</param>
        public void PostTopic(string aTopic, object aObj)
        {
            _stream.Position = 0;
            _formatter.Serialize(_stream, aObj);
            long length = _stream.Position;
            byte[] data = new byte[length];
            _stream.Position = 0;
            _stream.Read(data, 0, (int)length);

            try
            {
                Produce(String.Format("topic://{0}", aTopic), data);
            }
            catch (Exception exp)
            {
                Log4.LogHelper.Write(Log4.LogLevel.Error, 
                    string.Format("MQPublisher.PostTopic�쳣��Topic��{0}���쳣��Ϣ��{1}", aTopic, exp.ToString())
                    );
            }
        }
    }
}
