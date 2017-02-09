
using System.Collections.Generic;
using System.Threading;

namespace AnalysisManagerBase
{

    // delegate that does the eventual posting
    public delegate void MessageSenderDelegate(string message);

    class clsMessageQueueLogger
    {
        // actual delegate registers here
        public event MessageSenderDelegate Sender;

        // the worker thread that pulls messages off the queue
        // and posts them

        private readonly Thread worker;
        // synchronization and signalling stuff to coordinate
        // with worker thread
        private readonly EventWaitHandle waitHandle = new AutoResetEvent(false);

        private readonly object locker = new object();
        // local queue that contains messages to be sent

        private readonly Queue<string> m_statusMessages = new Queue<string>();

        public clsMessageQueueLogger()
        {
            worker = new Thread(PostalWorker);
            worker.Start();
        }

        // stuff status message onto the local status message queue 
        // to be sent off by the PostalWorker thread
        public void LogStatusMessage(string statusMessage)
        {
            lock (locker)
            {
                m_statusMessages.Enqueue(statusMessage);
            }
            waitHandle.Set();
        }

        // worker that runs in the worker thread
        // It pulls messages off the queue and posts
        // them via the delegate.  When no more messages
        // it waits until signalled by new message added to queue
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