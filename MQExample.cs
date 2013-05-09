using System;
using System.Collections.Generic;
using System.Text;

namespace xQuant.MQ
{
    class MQExample
    {
        /* 接收端例子
        [STAThread]
        static void Main(string[] args)
        {
            // 测试异步接收主题
            DataCache dataCache = DataCache.GetSingleton();
            dataCache.InitTopicList();
            Console.ReadKey(true);

            Console.WriteLine("sync receive start...");
            MQReceiver mqReceiver = new MQReceiver();
            if (!mqReceiver.Init(MQConst.QmgrHostName, MQConst.QmgrName, MQConst.QueueName))
            {
                Console.WriteLine("MQReceiver init err.");
                dataCache.CloseTopicList();
                return;
            }

            MQReceiver mqReceiverDelete = new MQReceiver();
            if (!mqReceiverDelete.Init(MQConst.QmgrHostName, MQConst.QmgrName, MQConst.QueueName))
            {
                Console.WriteLine("MQReceiver init err.");
                dataCache.CloseTopicList();
                return;
            }

            // 同步消息选择器
            MQMessage mqm1 = new MQMessage();
            mqm1.HeaderMcd.McdType = "REQUEST";
            mqm1.HeaderUser.UserBaseDate.Value = "20071121";
            string selector1 = mqm1.ParseSelector();

            int cnt = 0;
            while (true)
            {
                MQMessage mqm = mqReceiver.ReceiveMessage(10000, selector1);
                if (mqm == null)
                    break;

                Console.WriteLine("-----------------");
                Console.WriteLine("count:         {0}", cnt++);
                Console.WriteLine("msg text:      {0}", mqm.Text);
                Console.WriteLine("mcdtype:       {0}", mqm.HeaderMcd.McdType);
                Console.WriteLine("messageid:     {0}", mqm.HeaderMcd.MessageId);
                Console.WriteLine("serviceid:     {0}", mqm.HeaderUser.UserServiceId.Value);
                Console.WriteLine("servicestatus: {0}", mqm.HeaderUser.UserServiceStatus.Value);
                Console.WriteLine("serviceguage:  {0}", mqm.HeaderUser.UserServiceGuage.Value);
                Console.WriteLine("basedate:      {0}", mqm.HeaderUser.UserBaseDate.Value);
                foreach (MQParameter<string> list in mqm.HeaderUser.UserDefined)
                {
                    Console.WriteLine("properties:    {0} - {1}", list.Name, list.Value);
                }
            }

            // 异步消息选择器
            MQMessage mqm2 = new MQMessage();
            mqm2.Clear();
            mqm2.HeaderMcd.McdType = "REQUEST";
            mqm2.HeaderUser.Add("U_OTHER2", "other2");
            string selector2 = mqm2.ParseSelector();

            Console.WriteLine("async receive start...");
            mqReceiver.ReceiveMessage(dataCache, selector2);

            System.Threading.Thread.Sleep(10000);
            Console.WriteLine("press any key to browser message ...");
            Console.ReadKey(true);

            // 同步浏览消息选择器
            MQMessage mqm3 = new MQMessage();
            mqm3.Clear();
            mqm3.HeaderMcd.McdType = "REQUEST";
            mqm3.HeaderUser.UserServiceStatus.Value = "2";
            string selector3 = mqm3.ParseSelector();

            mqReceiver.BrowserMessage(selector3);
            cnt = 0;
            int times = 0;
            do
            {
                Console.WriteLine("time 1: {0}", DateTime.Now);
                MQMessage mqm = mqReceiver.PickMessage();
                Console.WriteLine("time 2: {0}", DateTime.Now);
                if (mqm == null)
                {
                    Console.WriteLine("seek message delay {0}", times);
                    System.Threading.Thread.Sleep(1000);
                    times++;
                }
                else
                {
                    Console.WriteLine("-----------------");
                    Console.WriteLine("count:         {0}", cnt++);
                    Console.WriteLine("mcdtype:       {0}", mqm.HeaderMcd.McdType);
                    Console.WriteLine("messageid:     {0}", mqm.HeaderMcd.MessageId);
                    Console.WriteLine("serviceid:     {0}", mqm.HeaderUser.UserServiceId.Value);
                    Console.WriteLine("servicestatus: {0}", mqm.HeaderUser.UserServiceStatus.Value);
                    Console.WriteLine("serviceguage:  {0}", mqm.HeaderUser.UserServiceGuage.Value);
                    Console.WriteLine("basedate:      {0}", mqm.HeaderUser.UserBaseDate.Value);
                    foreach (MQParameter<string> list in mqm.HeaderUser.UserDefined)
                    {
                        Console.WriteLine("properties:    {0} - {1}", list.Name, list.Value);
                    }
                    Console.WriteLine("msg text:      {0}", mqm.Text);

                    Console.WriteLine("time 3: {0}", DateTime.Now);
                    mqReceiverDelete.DeleteMessage(mqm);
                    Console.WriteLine("time 4: {0}", DateTime.Now);
                }
            } while (times < 10);

            Console.WriteLine("receiver end.");
            Console.ReadKey(true);

            mqReceiver.UnReceiveMessage(selector3);
            mqReceiver.Close();
            dataCache.CloseTopicList();

            return;
        }
        */

        /* 发送端例子
        [STAThread]
        static void Main(string[] args)
        {
            MQPublisher mqPublisher = new MQPublisher();
            if (!mqPublisher.Init(MQConst.QmgrHostName, MQConst.QmgrName))
            {
                Console.WriteLine("create mqm error");
                return;
            }

            MQBizData mqBizData = new MQBizData();
            mqBizData.Init();

            List<MQTopic> mqTopics = new List<MQTopic>();
            mqTopics.Add(new MQTopic(@"deal\*"));
            mqTopics.Add(new MQTopic(@"order\*"));
            mqTopics.Add(new MQTopic(@"pos\*"));

            Console.WriteLine("start to post...");

            // 提交主题列表
            System.Threading.Thread.Sleep(1000);
            mqPublisher.PostTopic<MQTopic>(new MQTopic(@"topic\testing"), mqTopics);
            Console.WriteLine("post MQTopic success...");

            // 提交Order列表
            System.Threading.Thread.Sleep(1000);
            for (int i = 0; i < MQConst.LoopTimes; i++)
            {
                mqPublisher.PostTopic<BizOrder>(new MQTopic(@"order\testing"), mqBizData.ListBizOrder);
            }
            Console.WriteLine("post BizOrder success...");

            // 提交Deal列表
            System.Threading.Thread.Sleep(1000);
            for (int i = 0; i < MQConst.LoopTimes; i++)
            {
                mqPublisher.PostTopic<BizDeal>(new MQTopic(@"deal\testing"), mqBizData.ListBizDeal);
            }
            Console.WriteLine("post BizDeal success...");

            // 提交Pos列表
            System.Threading.Thread.Sleep(1000);
            for (int i = 0; i < MQConst.LoopTimes; i++)
            {
                mqPublisher.PostTopic<BizPos>(new MQTopic(@"pos\testing"), mqBizData.ListBizPos);
            }
            Console.WriteLine("post BizPos success...");

            // 删除主题
            mqTopics.Clear();
            mqTopics.Add(new MQTopic(@"delete\order\*"));

            System.Threading.Thread.Sleep(5000);
            mqPublisher.PostTopic<MQTopic>(new MQTopic(@"topic\testing"), mqTopics);
            Console.WriteLine("repost MQTopic success...");

            // 再次提交Order列表
            System.Threading.Thread.Sleep(1000);
            for (int i = 0; i < MQConst.LoopTimes; i++)
            {
                mqPublisher.PostTopic<BizOrder>(new MQTopic(@"order\testing"), mqBizData.ListBizOrder);
            }
            Console.WriteLine("repost BizOrder success...");

            mqPublisher.Close();

            // 发送消息
            MQSender mqSender = new MQSender();
            if (!mqSender.Init(MQConst.QmgrHostName, MQConst.QmgrName, MQConst.QueueName))
            {
                Console.WriteLine("create mqm error");
                return;
            }

            // 第一条
            System.Threading.Thread.Sleep(1000);
            MQMessage mqm = new MQMessage("xEQ.Server example 1...");
            mqm.HeaderMcd.McdType = "REQUEST";
            mqm.HeaderUser.UserServiceId.Value = "123487293842833";
            mqm.HeaderUser.UserServiceStatus.Value = "0";
            mqm.HeaderUser.UserServiceGuage.Value = "88888";
            mqm.HeaderUser.UserBaseDate.Value = "20071121";
            mqm.HeaderUser.Add("U_OTHER", "other");
            for (int i = 0; i < MQConst.LoopTimes; i++)
            {
                mqSender.SendMessage(mqm);
            }
            Console.WriteLine("send 1st message success...");

            // 第二条
            System.Threading.Thread.Sleep(12000);
            mqm.Clear();
            mqm.HeaderMcd.McdType = "REQUEST";
            mqm.HeaderUser.UserServiceId.Value = "jhkguijknjk";
            mqm.HeaderUser.UserServiceStatus.Value = "1";
            mqm.HeaderUser.UserServiceGuage.Value = "99999";
            mqm.HeaderUser.UserBaseDate.Value = "20071122";
            mqm.HeaderUser.Add("U_OTHER2", "other2");
            mqm.Text = "xEQ.Server example 2...";
            for (int i = 0; i < MQConst.LoopTimes; i++)
            {
                mqSender.SendMessage(mqm);
            }
            Console.WriteLine("send 2nd message success...");

            // 第三条
            System.Threading.Thread.Sleep(12000);
            mqm.Clear();
            mqm.HeaderMcd.McdType = "REQUEST";
            mqm.HeaderUser.UserServiceId.Value = "sdcdssdc";
            mqm.HeaderUser.UserServiceStatus.Value = "2";
            mqm.HeaderUser.UserServiceGuage.Value = "666666";
            mqm.HeaderUser.UserBaseDate.Value = "20071123";
            mqm.HeaderUser.Add("U_OTHER3", "other3");
            mqm.Text = "xEQ.Server example 3...";
            for (int i = 0; i < 10; i++)
            {
                mqSender.SendMessage(mqm);

                if (i == MQConst.LoopTimes)
                    System.Threading.Thread.Sleep(6000);
            }
            Console.WriteLine("send 3rd message success...");

            mqSender.Close();

            Console.WriteLine("ok...");
            Console.ReadKey(true);
            return;
        }
        */
    }
}
