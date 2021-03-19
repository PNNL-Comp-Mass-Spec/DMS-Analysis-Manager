namespace AnalysisManagerBase
{
    /// <summary>
    /// Class that tracks message text and an associated manager name
    /// </summary>
    public class MessageContainer
    {
        /// <summary>
        /// Manager name associated with the message
        /// </summary>
        /// <remarks>If an empty string, will use the processor name sent to MessageSender when instantiating</remarks>
        public string ManagerName { get; }

        /// <summary>
        /// Message to log
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message"></param>
        /// <param name="managerName"></param>
        public MessageContainer(string message, string managerName)
        {
            Message = message;
            ManagerName = managerName;
        }
    }
}
