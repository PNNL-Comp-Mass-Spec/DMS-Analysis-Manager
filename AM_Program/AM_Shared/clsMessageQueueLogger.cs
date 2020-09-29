
using PRISM;
using System.Collections.Generic;
using System.Threading;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Delegate that does the eventual posting
    /// </summary>
    /// <param name="messageContainer"></param>
    public delegate void MessageSenderDelegate(clsMessageContainer messageContainer);

    /// <summary>
    /// Class for interacting with a message queue
    /// </summary>
    internal class clsMessageQueueLogger : EventNotifier
    {
        /// <summary>
        /// Actual delegate registers here
        /// </summary>
        public event MessageSenderDelegate Sender;

        /// <summary>
        /// The worker thread that pulls messages off the queue and posts them
        /// </summary>
        private Thread worker;

        /// <summary>
        /// Synchronization and signalling stuff to coordinate with worker thread
        /// </summary>
        private readonly EventWaitHandle waitHandle = new AutoResetEvent(false);

        private readonly object locker = new object();

        /// <summary>
        /// local queue that contains messages to be sent
        /// </summary>
        private readonly Queue<clsMessageContainer> mstatusMessages = new Queue<clsMessageContainer>();

        public clsMessageQueueLogger()
        {
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
            var messageContainer = new clsMessageContainer(statusMessage, managerName);

            lock (locker)
            {
                mstatusMessages.Enqueue(messageContainer);
            }

            if (!worker.IsAlive)
            {
                OnWarningEvent("Restarting PostalWorker thread in the MessageQueueLogger");
                StartWorkerThread();
            }

            waitHandle.Set();
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
                clsMessageContainer messageContainer = null;
                lock (locker)
                {
                    if (mstatusMessages.Count > 0)
                    {
                        messageContainer = mstatusMessages.Dequeue();
                        if (messageContainer?.Message == null)
                        {
                            return;
                        }
                    }
                }
                if (messageContainer?.Message != null)
                {
                    // we have work to do
                    // use our delegates (if we have any)
                    Sender?.Invoke(messageContainer);
                }
                else
                {
                    // No more mStatusMessages - wait for a signal
                    waitHandle.WaitOne(60000);
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
                waitHandle.Close();
            }
        }
    }
}