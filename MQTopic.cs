/*********************************************************************
 * ���ƣ� MQTopic.cs
 * ���ܣ� ���ⶨ��
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.21
 * ���£� 
 * �汾�� 1.0.0
 * �޶���
 *        1.0.0 --- ��������MQ������
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;

namespace xQuant.MQ
{
    /// <summary>
    /// MQ������
    /// </summary>
    [Serializable]
    public class MQTopic
    {
        ///// <summary>
        ///// ��̬�����
        ///// </summary>
        //public const string STATIC_DATA = "STATIC_TOPIC";
        ///// <summary>
        ///// ��̬�������͵��ڲ��ʽ��˻������ǰ׺
        ///// </summary>
        //public const string DYNAMIC_TOPIC_CASH = "DYNAMIC_TOPIC_CASH";
        ///// <summary>
        ///// ��̬�������͵��ڲ�֤ȯ�˻������ǰ׺
        ///// </summary>
        //public const string DYNAMIC_TOPIC_SECU = "DYNAMIC_TOPIC_SECU";

        ///// <summary>
        ///// ���������б����������
        ///// </summary>
        //public const string DYNAMIC_TOPIC_TOPIC = "DYNAMIC_TOPIC_TOPIC";
        
        // ��������
        private string _TopicName;

        /// <summary>
        /// ����MQ�������
        /// </summary>
        /// <param name="aTopicName">��������</param>
        public MQTopic(string aTopicName)
        {
            if (string.IsNullOrEmpty(aTopicName))
                throw new Exception("MQ�޷�������,���������Null �� ����''");
            _TopicName = aTopicName;
        }

        ///// <summary>
        ///// ����MQ�������
        ///// </summary>
        ///// <param name="aTypeInfo">������Ϣ</param>
        //public MQTopic(Type aTypeInfo)
        //{
        //    if (aTypeInfo == null)
        //        throw new Exception("MQ�޷�������,���������Null");
        //    _TopicName = aTypeInfo.ToString();
        //}

        ///// <summary>
        ///// ����MQ�������
        ///// </summary>
        ///// <param name="aList">����aList������б�ָܷ����������</param>
        //public MQTopic(List<string> aList)
        //{
        //    if (aList == null || aList.Count <= 0)
        //        throw new Exception("MQ�޷�������,���������Null �� ���ȵ���0");
        //    StringBuilder sb = new StringBuilder();
        //    sb.AppendFormat("{0}", aList[0]);
        //    for (int i = 1; i < aList.Count; i++)
        //    {
        //        sb.AppendFormat(@"\{0}", aList[i]);
        //    }
        //    _TopicName = sb.ToString();
        //}

        /// <summary>
        /// ��������
        /// </summary>
        public string TopicName
        {
            get { return _TopicName; }
        }

        /// <summary>
        /// ���ص��ں���
        /// </summary>
        /// <param name="obj">�Ƚ϶���</param>
        /// <returns>�����жϽ��</returns>
        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            if (this.GetType() != obj.GetType())
                return false;

            if (this == obj)
                return true;

            return TopicName.Equals(((MQTopic)obj).TopicName);
        }

        /// <summary>
        /// ���ػ�ȡ��ϣ���뷽��
        /// </summary>
        /// <returns>��ϣ����</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
