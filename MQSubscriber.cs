/*********************************************************************
 * ���ƣ� MQSubscriber.cs
 * ���ܣ� ���ⶩ����
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.21
 * ���£� 
 * �汾�� 1.1.0
 * �޶���
 *        1.0.0 --- ��ͬ��/�첽��ʽ��������
 *        1.1.0 --- �κ����ع����룬By 2009-04-11
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
    /// �ͻ��˶�����
    /// </summary>
    public class MQSubscriber : MQConsumer
    {
        private Dictionary<string, MQTopicCallBack> _callbackDict;
        private BinaryFormatter _formatter = new BinaryFormatter();

        /// <summary>
        /// ��ע���������
        /// </summary>
        public int TopicCount { get { return _callbackDict.Count; } }


        /// <summary>
        /// ���충����
        /// </summary>
        public MQSubscriber()
            : base()
        {
            _callbackDict = new Dictionary<string, MQTopicCallBack>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="aIsDurable">�Ƿ��ǿɳ־û��Ķ���ģʽ</param>
        public MQSubscriber(bool aIsDurable)
            : base(aIsDurable)
        {
            _callbackDict = new Dictionary<string, MQTopicCallBack>();
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
        /// ͬ����������
        /// </summary>
        /// <param name="aTopic">����</param>
        /// <param name="aTimeout">��ʱ��������0��ʾһֱ�ȴ�</param>
        /// <returns>��������</returns>
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
        /// ע�ᶩ������
        /// </summary>
        /// <param name="aTopic">����</param>
        /// <param name="aTopicSubscribeCallBack">���ⶩ�Ļص�����</param>
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
        /// ע�������б�
        /// </summary>
        /// <param name="aList">�����б�</param>
        /// <param name="aTopicSubscribeCallBack">���ⶩ�Ļص�����</param>
        public void RegisterTopic(List<MQTopic> aList, TopicSubscribeCallBack aTopicSubscribeCallBack)
        {
            // ��ͣ�ã���ע������ȽϿ�
            Connection.Stop();
            try
            {
                // ������������
                foreach (MQTopic topic in aList)
                {
                    RegisterTopic(topic, aTopicSubscribeCallBack);
                }

                // ɾ����ע�ᵫ�б���û�е�����
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

                    // ���б����Ƴ�����
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
        /// ȡ����������
        /// </summary>
        /// <param name="aTopic">����</param>
        public void UnRegisterTopic(MQTopic aTopic)
        {
            lock (_callbackDict)
            {
                _callbackDict.Remove(aTopic.TopicName);
            }

            // ʹ�ÿ���ȡ��ͬ��/�첽�µĶ���
            UnConsume(String.Format("topic://{0}", aTopic.TopicName), String.Empty);
        }

        /// <summary>
        /// �ж��Ƿ������Ҫ�޸ĵ�����
        /// </summary>
        /// <param name="listTopic">�������б�</param>
        /// <returns>���</returns>
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
        /// �ж������Ƿ��Ѿ�ע��
        /// </summary>
        /// <param name="topic">����</param>
        /// <returns>���</returns>
        public bool IsRegistered(MQTopic topic)
        {
            lock (_callbackDict)
            {
                return _callbackDict.ContainsKey(topic.TopicName);
            }
        }


        /// <summary>
        /// �ͷ���Դ
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
        /// �����Ѷ�������Ļص�����
        /// </summary>
        /// <param name="msg">�Ѷ��ĵ���Ϣ</param>
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
