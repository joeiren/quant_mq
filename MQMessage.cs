/*********************************************************************
 * 名称： MQMessage.cs
 * 功能： 消息定义
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.21
 * 最新： 
 * 版本： 1.0.0
 * 修订：
 *        1.0.0 --- 支持MCD和USER块的消息
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using IBM.XMS;

namespace xQuant.MQ
{
    /// <summary>
    /// MQ消息定义
    /// </summary>
    public class MQMessage
    {
        private MQHeaderMcd _headerMcd;
        private MQHeaderUser _headerUser;
        private string _text;
        private byte[] _byte;

        /// <summary>
        /// 构造MQ消息对象
        /// </summary>
        public MQMessage()
        {
            _headerMcd = new MQHeaderMcd();
            _headerUser = new MQHeaderUser();
            _text = null;
            _byte = null;
        }

        /// <summary>
        /// 构造文本格式的MQ消息对象
        /// </summary>
        /// <param name="aText">文本</param>
        public MQMessage(string aText)
        {
            _headerMcd = new MQHeaderMcd();
            _headerUser = new MQHeaderUser();
            _text = aText;
            _byte = null;
        }

        /// <summary>
        /// 构造二进制格式的MQ消息对象
        /// </summary>
        /// <param name="aByte">二进制数据</param>
        public MQMessage(byte[] aByte)
        {
            _headerMcd = new MQHeaderMcd();
            _headerUser = new MQHeaderUser();
            _text = null;
            _byte = aByte;
        }


        /// <summary>
        /// MCD消息头
        /// </summary>
        public MQHeaderMcd HeaderMcd { get { return _headerMcd; } }

        /// <summary>
        /// USER消息头
        /// </summary>
        public MQHeaderUser HeaderUser { get { return _headerUser; } }

        /// <summary>
        /// 消息文本
        /// </summary>
        public string Text
        {
            get { return _text; }
            set { _text = value; }
        }

        /// <summary>
        /// 消息主体
        /// </summary>
        public byte[] Byte
        {
            get { return _byte; }
            set { _byte = value; }
        }

        /// <summary>
        /// 清理消息所有消息
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
        /// 从JMS消息中分析出MQ消息内容
        /// </summary>
        /// <param name="aMessage">消息</param>
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
        /// 将MQ消息内容放入JMS消息
        /// </summary>
        /// <param name="aMessage">消息</param>
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
        /// 根据MessageId生成选择器
        /// </summary>
        /// <param name="aMessageId">JMS消息ID</param>
        /// <returns>选择器</returns>
        public static string ParseSelector(string aMessageId)
        {
            return String.Format("{0}='{1}'", XMSC.JMS_MESSAGEID, aMessageId);
        }


        /// <summary>
        /// 根据MQ消息头生成选择器
        /// </summary>
        /// <param name="mqMessage">MQMessage</param>
        /// <returns>返回选择器</returns>
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


        #region 消息头定义

        /// <summary>
        /// MCD消息头
        /// </summary>
        public class MQHeaderMcd
        {
            /// <summary>
            /// 请求消息
            /// </summary>
            public const string Request = "REQUEST";
            
            /// <summary>
            /// 响应消息
            /// </summary>
            public const string Response = "RESPONSE";
            
            /// <summary>
            /// 结果消息
            /// </summary>
            public const string Result = "RESULT";

            /// <summary>
            /// 类型
            /// </summary>
            public string McdType
            {
                get { return _mcdType; }
                set { _mcdType = value; }
            }

            /// <summary>
            /// 消息ID
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
        /// USER自定义消息头
        /// </summary>
        public class MQHeaderUser
        {
            /// <summary>
            /// 服务ID
            /// </summary>
            public MQParameter<string> UserServiceId { get { return _userServiceId; } }

            /// <summary>
            /// 任务编码
            /// </summary>
            public MQParameter<string> UserTaskCode { get { return _userTaskCode; } }

            /// <summary>
            /// 服务状态
            /// </summary>
            public MQParameter<string> UserServiceStatus { get { return _userServiceStatus; } }

            /// <summary>
            /// 进度，最大值为100
            /// </summary>
            public MQParameter<string> UserServiceGuage { get { return _userServiceGuage; } }

            /// <summary>
            /// 进度信息
            /// </summary>
            public MQParameter<string> UserServiceGuageInfo { get { return _userServiceGuageInfo; } }

            /// <summary>
            /// 基准日期
            /// </summary>
            public MQParameter<string> UserBaseDate { get { return _userBaseDate; } }

            /// <summary>
            /// 用户ID
            /// </summary>
            public MQParameter<string> UserUserId { get { return _userUserId; } }

            /// <summary>
            /// 自定义string类型
            /// </summary>
            public List<MQParameter<string>> UserDefined { get { return _userDefined; } }

            /// <summary>
            /// 添加自定义string型
            /// </summary>
            /// <param name="aName">名称</param>
            /// <param name="aValue">值</param>
            public void Add(string aName, string aValue)
            {
                if (aName.StartsWith(MQConst.JMS_FLAG))
                    throw new ArgumentException("自定义对象不能以(" + MQConst.JMS_FLAG + ")开头");

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
            /// 清除
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
