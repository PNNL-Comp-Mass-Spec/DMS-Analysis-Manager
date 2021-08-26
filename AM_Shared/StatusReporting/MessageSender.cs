using System;
using System.Collections.Generic;
using Apache.NMS;
using PRISM;

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Sends messages to ActiveMQ message broker using NMS client library
    /// </summary>
    internal class MessageSender
    {
        private readonly string mTopicName;
        private readonly string mBrokerUri;

        private readonly string mProcessorName;
        private IConnection mConnection;
        private ISession mSession;

        private IMessageProducer mProducer;
        private bool mIsDisposed;

        private bool mHasConnection;

        public MessageSender(string brokerUri, string topicName, string processorName)
        {
            mTopicName = topicName;
            mBrokerUri = brokerUri;
            mProcessorName = processorName;
        }

        /// <summary>
        /// Send the message using NMS connection objects
        /// </summary>
        /// <remarks>
        /// If connection does not exist, make it
        /// If connection objects don't work, erase them and make another set
        /// </remarks>
        /// <param name="messageContainer"></param>
        public void SendMessage(MessageContainer messageContainer)
        {
            if (mIsDisposed)
            {
                return;
            }

            if (!mHasConnection)
            {
                CreateConnection();
            }

            if (!mHasConnection)
            {
                return;
            }

            try
            {
                var textMessage = mSession.CreateTextMessage(messageContainer.Message);
                textMessage.NMSTimeToLive = TimeSpan.FromMinutes(60);
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                textMessage.Properties.SetString("ProcessorName",
                                                 string.IsNullOrWhiteSpace(messageContainer.ManagerName) ? mProcessorName : messageContainer.ManagerName);

                mProducer.Send(textMessage);
            }
            catch (Exception ex)
            {
                // something went wrong trying to send message,
                // get rid of current set of connection object - they'll never work again anyway
                ConsoleMsgUtils.ShowDebug("Exception contacting the ActiveMQ server: " + ex.Message);
                DestroyConnection();
            }
        }

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount"></param>
        /// <param name="timeoutSeconds"></param>
        private void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (mHasConnection)
            {
                return;
            }

            if (retryCount < 0)
            {
                retryCount = 0;
            }

            var retriesRemaining = retryCount;

            if (timeoutSeconds < 5)
            {
                timeoutSeconds = 5;
            }

            var errorList = new List<string>();

            while (retriesRemaining >= 0)
            {
                try
                {
                    var connectionFactory = new Apache.NMS.ActiveMQ.ConnectionFactory(mBrokerUri, mProcessorName)
                    {
                        AcknowledgementMode = AcknowledgementMode.AutoAcknowledge
                    };

                    mConnection = connectionFactory.CreateConnection();
                    mConnection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    mConnection.Start();

                    mSession = mConnection.CreateSession();

                    mProducer = mSession.CreateProducer(new Apache.NMS.ActiveMQ.Commands.ActiveMQTopic(mTopicName));
                    mHasConnection = true;

                    return;
                }
                catch (Exception ex)
                {
                    // Connection failed
                    if (!errorList.Contains(ex.Message))
                    {
                        errorList.Add(ex.Message);
                    }

                    // Sleep for 3 seconds
                    Global.IdleLoop(3);
                }

                retriesRemaining--;
            }

            // If we get here, we never could connect to the message broker

            var msg = "Exception creating broker connection";
            if (retryCount > 0)
            {
                msg += " after " + (retryCount + 1) + " attempts";
            }

            msg += ": " + string.Join("; ", errorList);
            OnErrorEvent(msg);
        }

        private void DestroyConnection()
        {
            try
            {
                if (mHasConnection)
                {
                    mConnection?.Close();
                    mHasConnection = false;
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        public void Dispose()
        {
            if (!mIsDisposed)
            {
                mIsDisposed = true;
                try
                {
                    DestroyConnection();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
        }

        public event ErrorEventEventHandler ErrorEvent;
        public delegate void ErrorEventEventHandler(string message, Exception ex);

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        protected void OnErrorEvent(string message)
        {
            ErrorEvent?.Invoke(message, null);
        }
    }
}
