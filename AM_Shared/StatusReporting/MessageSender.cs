using System;
using System.Collections.Generic;
using Apache.NMS;
using PRISM;

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Sends messages to ActiveMQ message broker using NMS client library
    /// </summary>
    internal class MessageSender : EventNotifier, IDisposable
    {
        private readonly string mBrokerUri;

        private IConnection mConnection;

        private bool mHasConnection;

        private bool mIsDisposed;

        private readonly string mProcessorName;

        private IMessageProducer mProducer;

        private ISession mSession;

        private readonly string mTopicName;

        /// <summary>
        /// This is incremented each time this class is unable to connect to the message broker
        /// It is reset to 0 when a successful connection is made
        /// </summary>
        public int BrokerConnectionFailures { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerUri">Message broker URI</param>
        /// <param name="topicName">Topic name</param>
        /// <param name="processorName">Manager name</param>
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
        /// <param name="messageContainer">Message container</param>
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
                textMessage.Properties.SetString(
                    "ProcessorName",
                    string.IsNullOrWhiteSpace(messageContainer.ManagerName) ? mProcessorName : messageContainer.ManagerName);

                mProducer.Send(textMessage);

                BrokerConnectionFailures = 0;
            }
            catch (Exception ex)
            {
                // something went wrong trying to send message,
                // get rid of current set of connection object - they'll never work again anyway
                ConsoleMsgUtils.ShowDebug("Exception contacting the ActiveMQ server: " + ex.Message);
                DestroyConnection();

                BrokerConnectionFailures++;
            }
        }

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount">Retry count</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
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
                    BrokerConnectionFailures = 0;

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
                    ConsoleMsgUtils.SleepSeconds(3);
                }

                retriesRemaining--;
            }

            // If we get here, we never could connect to the message broker

            // Example formatted message:
            // Exception creating broker connection after 3 attempts: Error connecting to proto-7.pnl.gov:61616

            var attemptDescription = retryCount == 0 ? string.Empty : " after " + (retryCount + 1) + " attempts";

            OnErrorEvent("Exception creating broker connection{0}: {1}", attemptDescription, string.Join("; ", errorList));

            BrokerConnectionFailures++;
        }

        private void DestroyConnection()
        {
            try
            {
                if (!mHasConnection)
                    return;

                mConnection?.Close();
                mHasConnection = false;
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        public void Dispose()
        {
            if (mIsDisposed)
                return;

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
}
