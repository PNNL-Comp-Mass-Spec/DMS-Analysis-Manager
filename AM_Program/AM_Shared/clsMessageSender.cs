
using System;
using System.Collections.Generic;
using Apache.NMS;

namespace AnalysisManagerBase
{

    /// <summary>
    /// Sends messages to ActiveMQ message broker using NMS client library
    /// </summary>
    /// <remarks></remarks>
    class clsMessageSender
    {

        private readonly string topicName;
        private readonly string brokerUri;

        private readonly string processorName;
        private IConnection connection;
        private ISession session;

        private IMessageProducer producer;
        private bool isDisposed;

        private bool hasConnection;

        public clsMessageSender(string brokerUri, string topicName, string processorName)
        {
            this.topicName = topicName;
            this.brokerUri = brokerUri;
            this.processorName = processorName;
        }

        /// <summary>
        ///  Send the message using NMS connection objects
        /// </summary>
        /// <param name="message"></param>
        /// <remarks>
        /// If connection does not exist, make it
        /// If connection objects don't work, erase them and make another set
        /// </remarks>
        public void SendMessage(string message)
        {
            if (isDisposed)
            {
                return;
            }
            if (!hasConnection)
            {
                CreateConnection();
            }
            if (hasConnection)
            {
                try
                {
                    var textMessage = session.CreateTextMessage(message);
                    textMessage.Properties.SetString("ProcessorName", processorName);
                    producer.Send(textMessage);
                }
                catch (Exception)
                {
                    // something went wrong trying to send message,
                    // get rid of current set of connection object - they'll never work again anyway
                    DestroyConnection();                    
                }
            }
        }

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount"></param>
        /// <param name="timeoutSeconds"></param>
        /// <remarks></remarks>
        protected void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (hasConnection)
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
                    var connectionFactory = new Apache.NMS.ActiveMQ.ConnectionFactory(brokerUri);
                    connection = connectionFactory.CreateConnection();
                    connection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    connection.Start();

                    session = connection.CreateSession();

                    producer = session.CreateProducer(new Apache.NMS.ActiveMQ.Commands.ActiveMQTopic(topicName));
                    hasConnection = true;

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
                    System.Threading.Thread.Sleep(3000);
                }

                retriesRemaining -= 1;
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

        protected void DestroyConnection()
        {
            try
            {
                if (hasConnection)
                {
                    producer?.Dispose();
                    session?.Dispose();
                    connection?.Dispose();
                    hasConnection = false;
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        public void Dispose()
        {
            if (!isDisposed)
            {
                isDisposed = true;
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

        #region "Events and Event Handlers"

        public event ErrorEventEventHandler ErrorEvent;
        public delegate void ErrorEventEventHandler(string strMessage, Exception ex);

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="strMessage"></param>
        protected void OnErrorEvent(string strMessage)
        {
            ErrorEvent?.Invoke(strMessage, null);
        }

        #endregion
    }
}