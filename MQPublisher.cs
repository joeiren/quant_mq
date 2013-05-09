/*********************************************************************
 * 名称： MQPublisher.cs
 * 功能： 主题发布者
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.21
 * 最新： 
 * 版本： 1.1.0
 * 修订：
 *        1.0.0 --- 发布主题
 *        1.1.0 --- 廖洪周重构代码，By 2009-04-11
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace xQuant.MQ
{
    /// <summary>
    /// 发布者
    /// </summary>
    public class MQPublisher : MQProducer
    {
        private BinaryFormatter _formatter = new BinaryFormatter();
        private MemoryStream _stream = new MemoryStream();

        /// <summary>
        /// 构造发布者
        /// </summary>
        public MQPublisher()
            : base(MQConst.MsgTimeToLive)
        {
        }

        /// <summary>
        /// 初始化订阅者，如果初始化失败，将抛出异常
        /// </summary>
        public void Init()
        {
            base.InitCore();
        }

        /// <summary>
        /// 初始化订阅者，如果初始化失败，将抛出异常
        /// </summary>
        /// <param name="mqConnection">MQ连接</param>
        public void Init(MQConnection mqConnection)
        {
            base.InitCore(mqConnection);
        }


        /// <summary>
        /// 提交发布主题
        /// </summary>
        /// <param name="aTopic">主题</param>
        /// <param name="aObj">主题类列表</param>
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
                    string.Format("MQPublisher.PostTopic异常，Topic：{0}，异常信息：{1}", aTopic, exp.ToString())
                    );
            }
        }
    }
}
