using System.Collections.Generic;
using System.Threading;
using PRISM;

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Class for interacting with a message queue
    /// </summary>
    internal class MessageQueueLogger : EventNotifier
    {
        /// <summary>
        /// Status file class
        /// </summary>
        /// <remarks>
        /// If there are errors connecting to the message queue, method
        /// </remarks>
        private readonly StatusFile mStatusFile;

        /// <summary>
        /// Message sender
        /// </summary>
        private readonly MessageSender mMessageSender;

        /// <summary>
        /// The worker thread that pulls messages off the queue and posts them
        /// </summary>
        private Thread worker;

        /// <summary>
        /// Synchronization and signaling stuff to coordinate with worker thread
        /// </summary>
        private readonly EventWaitHandle mWaitHandle = new AutoResetEvent(false);

        private readonly object mLocker = new();

        /// <summary>
        /// Local queue that contains messages to be sent
        /// </summary>
        private readonly Queue<MessageContainer> mStatusMessages = new();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="statusFile"></param>
        public MessageQueueLogger(MessageSender sender, StatusFile statusFile)
        {
            mStatusFile = statusFile;
            mMessageSender = sender;
            StartWorkerThread();
        }

        /// <summary>
        /// Push status message onto the local status message queue
        /// to be sent off by the PostalWorker thread
        /// </summary>
        /// <param name="statusMessage"></param>
        /// <param name="managerName"></param>
        public void LogStatusMessage(string statusMessage, string managerName)
        {
            var messageContainer = new MessageContainer(statusMessage, managerName);

            lock (mLocker)
            {
                mStatusMessages.Enqueue(messageContainer);
            }

            if (!worker.IsAlive)
            {
                OnWarningEvent("Restarting PostalWorker thread in the MessageQueueLogger");
                StartWorkerThread();
            }

            mWaitHandle.Set();
        }

        /// <summary>
        /// Worker that runs in the worker thread
        /// It pulls messages off the queue and posts
        /// them via the delegate.  When no more messages
        /// it waits until signaled by new message added to queue
        /// </summary>
        private void PostalWorker()
        {
            while (true)
            {
                MessageContainer messageContainer = null;
                int messagesRemaining;

                lock (mLocker)
                {
                    if (mStatusMessages.Count > 0)
                    {
                        messageContainer = mStatusMessages.Dequeue();

                        if (messageContainer?.Message == null)
                        {
                            return;
                        }
                    }

                    messagesRemaining = mStatusMessages.Count;
                }

                if (messageContainer?.Message != null)
                {
                    if (mMessageSender.BrokerConnectionFailures > 5)
                    {
                        // Too many failures connecting to the broker
                        // Enable directly sending status info to the database, but first make sure mStatusMessages is empty

                        if (messagesRemaining > 0)
                            continue;

                        mStatusFile.EnableBrokerDbLoggingNow();
                        continue;
                    }

                    mMessageSender.SendMessage(messageContainer);
                }
                else
                {
                    // No more mStatusMessages - wait for a signal
                    mWaitHandle.WaitOne(60000);
                }
            }
        }

        private void StartWorkerThread()
        {
            worker = new Thread(PostalWorker);
            worker.Start();
        }

        public void Dispose()
        {
            LogStatusMessage(null, string.Empty);

            // Signal the worker to exit.
            // Wait a maximum of 15 seconds
            if (!worker.Join(15000))
            {
                // Still no response, so kill the thread
                worker.Abort();
            }
            else
            {
                // Wait for the consumer's thread to finish.
                // Release any OS resources.
                mWaitHandle.Close();
            }
        }
    }
}