/*********************************************************************
 * 名称： MQTopic.cs
 * 功能： 主题定义
 * 作者： szx
 * 公司： xQuant
 * 日期： 2007.11.21
 * 最新： 
 * 版本： 1.0.0
 * 修订：
 *        1.0.0 --- 定义用于MQ的主题
 *********************************************************************/
using System;
using System.Collections.Generic;
using System.Text;

namespace xQuant.MQ
{
    /// <summary>
    /// MQ主题类
    /// </summary>
    [Serializable]
    public class MQTopic
    {
        ///// <summary>
        ///// 静态主题的
        ///// </summary>
        //public const string STATIC_DATA = "STATIC_TOPIC";
        ///// <summary>
        ///// 动态主题类型的内部资金账户主题的前缀
        ///// </summary>
        //public const string DYNAMIC_TOPIC_CASH = "DYNAMIC_TOPIC_CASH";
        ///// <summary>
        ///// 动态主题类型的内部证券账户主题的前缀
        ///// </summary>
        //public const string DYNAMIC_TOPIC_SECU = "DYNAMIC_TOPIC_SECU";

        ///// <summary>
        ///// 订阅主题列表的主题名称
        ///// </summary>
        //public const string DYNAMIC_TOPIC_TOPIC = "DYNAMIC_TOPIC_TOPIC";
        
        // 主题名称
        private string _TopicName;

        /// <summary>
        /// 构造MQ主题对象
        /// </summary>
        /// <param name="aTopicName">主题名称</param>
        public MQTopic(string aTopicName)
        {
            if (string.IsNullOrEmpty(aTopicName))
                throw new Exception("MQ无法被构造,构造参数是Null 或 等于''");
            _TopicName = aTopicName;
        }

        ///// <summary>
        ///// 构造MQ主题对象
        ///// </summary>
        ///// <param name="aTypeInfo">类型信息</param>
        //public MQTopic(Type aTypeInfo)
        //{
        //    if (aTypeInfo == null)
        //        throw new Exception("MQ无法被构造,构造参数是Null");
        //    _TopicName = aTypeInfo.ToString();
        //}

        ///// <summary>
        ///// 构造MQ主题对象
        ///// </summary>
        ///// <param name="aList">根据aList生产以斜杠分隔的主题对象</param>
        //public MQTopic(List<string> aList)
        //{
        //    if (aList == null || aList.Count <= 0)
        //        throw new Exception("MQ无法被构造,构造参数是Null 或 长度等于0");
        //    StringBuilder sb = new StringBuilder();
        //    sb.AppendFormat("{0}", aList[0]);
        //    for (int i = 1; i < aList.Count; i++)
        //    {
        //        sb.AppendFormat(@"\{0}", aList[i]);
        //    }
        //    _TopicName = sb.ToString();
        //}

        /// <summary>
        /// 主题名称
        /// </summary>
        public string TopicName
        {
            get { return _TopicName; }
        }

        /// <summary>
        /// 重载等于函数
        /// </summary>
        /// <param name="obj">比较对象</param>
        /// <returns>返回判断结果</returns>
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
        /// 重载获取哈希编码方法
        /// </summary>
        /// <returns>哈希编码</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
