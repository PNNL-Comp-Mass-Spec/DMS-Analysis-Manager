
using System.Collections.Generic;
using System.Threading;

namespace AnalysisManagerBase
{

    /// <summary>
    /// Delegate that does the eventual posting
    /// </summary>
    /// <param name="message"></param>
    public delegate void MessageSenderDelegate(string message);

    /// <summary>
    /// Class for interacting with a message queue
    /// </summary>
    class clsMessageQueueLogger
    {
        /// <summary>
        /// Actual delegate registers here
        /// </summary>
        public event MessageSenderDelegate Sender;

        /// <summary>
        /// The worker thread that pulls messages off the queue and posts them
        /// </summary>
        private readonly Thread worker;

        /// <summary>
        /// Synchronization and signalling stuff to coordinate with worker thread
        /// </summary>
        private readonly EventWaitHandle waitHandle = new AutoResetEvent(false);

        private readonly object locker = new object();

        /// <summary>
        /// local queue that contains messages to be sent
        /// </summary>
        private readonly Queue<string> m_statusMessages = new Queue<string>();

        public clsMessageQueueLogger()
        {
            worker = new Thread(PostalWorker);
            worker.Start();
        }

        /// <summary>
        /// Push status message onto the local status message queue 
        /// to be sent off by the PostalWorker thread
        /// </summary>
        /// <param name="statusMessage"></param>
        public void LogStatusMessage(string statusMessage)
        {
            lock (locker)
            {
                m_statusMessages.Enqueue(statusMessage);
            }
            waitHandle.Set();
        }

        /// <summary>
        /// Worker that runs in the worker thread
        /// It pulls messages off the queue and posts
        /// them via the delegate.  When no more messages
        /// it waits until signalled by new message added to queue
        /// </summary>
        private void PostalWorker()
        {
            while (true)
            {
                string statusMessage = null;
                lock (locker)
                {
                    if (m_statusMessages.Count > 0)
                    {
                        statusMessage = m_statusMessages.Dequeue();
                        if (statusMessage == null)
                        {
                            return;
                        }
                    }
                }
                if (statusMessage != null)
                {
                    // we have work to do
                    // use our delegates (if we have any)
                    Sender?.Invoke(statusMessage);
                }
                else
                {
                    waitHandle.WaitOne();
                    // No more m_statusMessages - wait for a signal
                }
            }
        }

        public void Dispose()
        {
            LogStatusMessage(null);
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