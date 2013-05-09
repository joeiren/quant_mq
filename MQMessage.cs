/*********************************************************************
 * ���ƣ� MQMessage.cs
 * ���ܣ� ��Ϣ����
 * ���ߣ� szx
 * ��˾�� xQuant
 * ���ڣ� 2007.11.21
 * ���£� 
 * �汾�� 1.0.0
 * �޶���
 *        1.0.0 --- ֧��MCD��USER�����Ϣ
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// MQ��Ϣ����
    /// </summary>
    public class MQMessage
    {
        private MQHeaderMcd _headerMcd;
        private MQHeaderUser _headerUser;
        private string _text;
        private byte[] _byte;

        /// <summary>
        /// ����MQ��Ϣ����
        /// </summary>
        public MQMessage()
        {
            _headerMcd = new MQHeaderMcd();
            _headerUser = new MQHeaderUser();
            _text = null;
            _byte = null;
        }

        /// <summary>
        /// �����ı���ʽ��MQ��Ϣ����
        /// </summary>
        /// <param name="aText">�ı�</param>
        public MQMessage(string aText)
        {
            _headerMcd = new MQHeaderMcd();
            _headerUser = new MQHeaderUser();
            _text = aText;
            _byte = null;
        }

        /// <summary>
        /// ��������Ƹ�ʽ��MQ��Ϣ����
        /// </summary>
        /// <param name="aByte">����������</param>
        public MQMessage(byte[] aByte)
        {
            _headerMcd = new MQHeaderMcd();
            _headerUser = new MQHeaderUser();
            _text = null;
            _byte = aByte;
        }


        /// <summary>
        /// MCD��Ϣͷ
        /// </summary>
        public MQHeaderMcd HeaderMcd { get { return _headerMcd; } }

        /// <summary>
        /// USER��Ϣͷ
        /// </summary>
        public MQHeaderUser HeaderUser { get { return _headerUser; } }

        /// <summary>
        /// ��Ϣ�ı�
        /// </summary>
        public string Text
        {
            get { return _text; }
            set { _text = value; }
        }

        /// <summary>
        /// ��Ϣ����
        /// </summary>
        public byte[] Byte
        {
            get { return _byte; }
            set { _byte = value; }
        }

        /// <summary>
        /// ������Ϣ������Ϣ
        /// </summary>
        public void Clear()
        {
            _text = null;
            _byte = null;
            _headerMcd.McdType = null;
            _headerMcd.MessageId = null;
            _headerUser.Clear();
        }

        /// <summary>
        /// ��JMS��Ϣ�з�����MQ��Ϣ����
        /// </summary>
        /// <param name="aMessage">��Ϣ</param>
        public void ParseMessage(IMessage aMessage)
        {
            if (aMessage == null)
                return;

            HeaderMcd.McdType = aMessage.JMSType;
            HeaderMcd.MessageId = aMessage.JMSMessageID;

            if (aMessage.PropertyExists(HeaderUser.UserServiceId.Name))
                HeaderUser.UserServiceId.Value = aMessage.GetStringProperty(HeaderUser.UserServiceId.Name);
            if (aMessage.PropertyExists(HeaderUser.UserServiceStatus.Name))
                HeaderUser.UserServiceStatus.Value = aMessage.GetStringProperty(HeaderUser.UserServiceStatus.Name);
            if (aMessage.PropertyExists(HeaderUser.UserServiceGuage.Name))
                HeaderUser.UserServiceGuage.Value = aMessage.GetStringProperty(HeaderUser.UserServiceGuage.Name);
            if (aMessage.PropertyExists(HeaderUser.UserBaseDate.Name))
                HeaderUser.UserBaseDate.Value = aMessage.GetStringProperty(HeaderUser.UserBaseDate.Name);
            if (aMessage.PropertyExists(HeaderUser.UserServiceGuageInfo.Name))
                HeaderUser.UserServiceGuageInfo.Value = aMessage.GetStringProperty(HeaderUser.UserServiceGuageInfo.Name);
            if (aMessage.PropertyExists(HeaderUser.UserTaskCode.Name))
                HeaderUser.UserTaskCode.Value = aMessage.GetStringProperty(HeaderUser.UserTaskCode.Name);
            if (aMessage.PropertyExists(HeaderUser.UserUserId.Name))
                HeaderUser.UserUserId.Value = aMessage.GetStringProperty(HeaderUser.UserUserId.Name);

            IEnumerator names = aMessage.PropertyNames;
            lock (names)
            {
                names.Reset();
                while (names.MoveNext())
                {
                    string name = (string)names.Current;
                    if (name.StartsWith(MQConst.JMS_FLAG))
                        continue;

                    switch (name)
                    {
                        case MQConst.USER_BASEDATE:
                        case MQConst.USER_SERVICEGUAGE:
                        case MQConst.USER_SERVICEID:
                        case MQConst.USER_SERVICESTATUS:
                        case MQConst.USER_SERVICEGUAGEINFO:
                        case MQConst.USER_TASKCODE:
                        case MQConst.USER_USERID:
                            break;

                        default:
                            HeaderUser.Add(name, aMessage.GetStringProperty(name));
                            break;
                    }
                }
            }

            if (aMessage is ITextMessage)
            {
                Text = ((ITextMessage)aMessage).Text;
            }
            else if (aMessage is IObjectMessage)
            {
                Byte = ((IObjectMessage)aMessage).GetObject();
            }
        }

        /// <summary>
        /// ��MQ��Ϣ���ݷ���JMS��Ϣ
        /// </summary>
        /// <param name="aMessage">��Ϣ</param>
        public void ToMessage(IMessage aMessage)
        {
            if (HeaderMcd.McdType != null)
                aMessage.JMSType = HeaderMcd.McdType;

            if (HeaderUser.UserServiceId.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserServiceId.Name, HeaderUser.UserServiceId.Value);
            if (HeaderUser.UserServiceStatus.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserServiceStatus.Name, HeaderUser.UserServiceStatus.Value);
            if (HeaderUser.UserServiceGuage.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserServiceGuage.Name, HeaderUser.UserServiceGuage.Value);
            if (HeaderUser.UserBaseDate.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserBaseDate.Name, HeaderUser.UserBaseDate.Value);
            if (HeaderUser.UserServiceGuageInfo.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserServiceGuageInfo.Name, HeaderUser.UserServiceGuageInfo.Value);
            if (HeaderUser.UserTaskCode.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserTaskCode.Name, HeaderUser.UserTaskCode.Value);
            if (HeaderUser.UserUserId.Value != null)
                aMessage.SetStringProperty(HeaderUser.UserUserId.Name, HeaderUser.UserUserId.Value);

            foreach (MQParameter<string> list in HeaderUser.UserDefined)
            {
                if (list.Value != null)
                    aMessage.SetStringProperty(list.Name, list.Value);
            }
        }

        /// <summary>
        /// ����MessageId����ѡ����
        /// </summary>
        /// <param name="aMessageId">JMS��ϢID</param>
        /// <returns>ѡ����</returns>
        public static string ParseSelector(string aMessageId)
        {
            return String.Format("{0}='{1}'", XMSC.JMS_MESSAGEID, aMessageId);
        }


        /// <summary>
        /// ����MQ��Ϣͷ����ѡ����
        /// </summary>
        /// <param name="mqMessage">MQMessage</param>
        /// <returns>����ѡ����</returns>
        public static string ParseSelector(MQMessage mqMessage)
        {
            StringBuilder selector = new StringBuilder();

            if (mqMessage.HeaderMcd.McdType != null)
                selector.AppendFormat("{0}='{1}'", XMSC.JMS_TYPE, mqMessage.HeaderMcd.McdType);
            if (mqMessage.HeaderMcd.MessageId != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", XMSC.JMS_MESSAGEID, mqMessage.HeaderMcd.MessageId);
            }

            if (mqMessage.HeaderUser.UserServiceId.Value != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", mqMessage.HeaderUser.UserServiceId.Name, mqMessage.HeaderUser.UserServiceId.Value);
            }
            if (mqMessage.HeaderUser.UserServiceStatus.Value != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", mqMessage.HeaderUser.UserServiceStatus.Name, mqMessage.HeaderUser.UserServiceStatus.Value);
            }
            if (mqMessage.HeaderUser.UserServiceGuage.Value != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", mqMessage.HeaderUser.UserServiceGuage.Name, mqMessage.HeaderUser.UserServiceGuage.Value);
            }
            if (mqMessage.HeaderUser.UserBaseDate.Value != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", mqMessage.HeaderUser.UserBaseDate.Name, mqMessage.HeaderUser.UserBaseDate.Value);
            }
            if (mqMessage.HeaderUser.UserUserId.Value != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", mqMessage.HeaderUser.UserUserId.Name, mqMessage.HeaderUser.UserUserId.Value);
            }
            if (mqMessage.HeaderUser.UserTaskCode.Value != null)
            {
                if (selector.Length > 0)
                    selector.Append(" and ");
                selector.AppendFormat("{0}='{1}'", mqMessage.HeaderUser.UserTaskCode.Name, mqMessage.HeaderUser.UserTaskCode.Value);
            }

            foreach (MQParameter<string> list in mqMessage.HeaderUser.UserDefined)
            {
                if (list.Value != null)
                {
                    if (selector.Length > 0)
                        selector.Append(" and ");
                    selector.AppendFormat("{0}='{1}'", list.Name, list.Value);
                }
            }

            return selector.ToString();
        }


        #region ��Ϣͷ����

        /// <summary>
        /// MCD��Ϣͷ
        /// </summary>
        public class MQHeaderMcd
        {
            /// <summary>
            /// ������Ϣ
            /// </summary>
            public const string Request = "REQUEST";
            
            /// <summary>
            /// ��Ӧ��Ϣ
            /// </summary>
            public const string Response = "RESPONSE";
            
            /// <summary>
            /// �����Ϣ
            /// </summary>
            public const string Result = "RESULT";

            /// <summary>
            /// ����
            /// </summary>
            public string McdType
            {
                get { return _mcdType; }
                set { _mcdType = value; }
            }

            /// <summary>
            /// ��ϢID
            /// </summary>
            public string MessageId
            {
                get { return _mcdMessageId; }
                set { _mcdMessageId = value; }
            }

            private string _mcdMessageId = null;
            private string _mcdType = null;
        }


        /// <summary>
        /// USER�Զ�����Ϣͷ
        /// </summary>
        public class MQHeaderUser
        {
            /// <summary>
            /// ����ID
            /// </summary>
            public MQParameter<string> UserServiceId { get { return _userServiceId; } }

            /// <summary>
            /// �������
            /// </summary>
            public MQParameter<string> UserTaskCode { get { return _userTaskCode; } }

            /// <summary>
            /// ����״̬
            /// </summary>
            public MQParameter<string> UserServiceStatus { get { return _userServiceStatus; } }

            /// <summary>
            /// ���ȣ����ֵΪ100
            /// </summary>
            public MQParameter<string> UserServiceGuage { get { return _userServiceGuage; } }

            /// <summary>
            /// ������Ϣ
            /// </summary>
            public MQParameter<string> UserServiceGuageInfo { get { return _userServiceGuageInfo; } }

            /// <summary>
            /// ��׼����
            /// </summary>
            public MQParameter<string> UserBaseDate { get { return _userBaseDate; } }

            /// <summary>
            /// �û�ID
            /// </summary>
            public MQParameter<string> UserUserId { get { return _userUserId; } }

            /// <summary>
            /// �Զ���string����
            /// </summary>
            public List<MQParameter<string>> UserDefined { get { return _userDefined; } }

            /// <summary>
            /// ����Զ���string��
            /// </summary>
            /// <param name="aName">����</param>
            /// <param name="aValue">ֵ</param>
            public void Add(string aName, string aValue)
            {
                if (aName.StartsWith(MQConst.JMS_FLAG))
                    throw new ArgumentException("�Զ����������(" + MQConst.JMS_FLAG + ")��ͷ");

                foreach (MQParameter<string> list in _userDefined)
                {
                    if (list.Name.Equals(aName))
                    {
                        _userDefined.Remove(list);
                        break;
                    }
                }

                MQParameter<string> _param = new MQParameter<string>(aName, aValue);
                _userDefined.Add(_param);
            }

            /// <summary>
            /// ���
            /// </summary>
            public void Clear()
            {
                _userServiceId.Value = null;
                _userServiceStatus.Value = null;
                _userServiceGuage.Value = null;
                _userBaseDate.Value = null;
                _userServiceGuageInfo.Value = null;
                _userUserId.Value = null;
                _userTaskCode.Value = null;
                _userDefined.Clear();
            }

            private MQParameter<string> _userServiceId = new MQParameter<string>(MQConst.USER_SERVICEID);
            private MQParameter<string> _userTaskCode = new MQParameter<string>(MQConst.USER_TASKCODE);
            private MQParameter<string> _userServiceStatus = new MQParameter<string>(MQConst.USER_SERVICESTATUS);
            private MQParameter<string> _userServiceGuage = new MQParameter<string>(MQConst.USER_SERVICEGUAGE);
            private MQParameter<string> _userServiceGuageInfo = new MQParameter<string>(MQConst.USER_SERVICEGUAGEINFO);
            private MQParameter<string> _userBaseDate = new MQParameter<string>(MQConst.USER_BASEDATE);
            private MQParameter<string> _userUserId = new MQParameter<string>(MQConst.USER_USERID);
            private List<MQParameter<string>> _userDefined = new List<MQParameter<string>>();
        }

        #endregion
    }
}
