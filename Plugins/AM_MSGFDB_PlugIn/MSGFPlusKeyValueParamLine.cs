using AnalysisManagerBase;

namespace AnalysisManagerMSGFDBPlugIn
{
    class MSGFPlusKeyValueParamFileLine : KeyValueParamFileLine
    {
        /// <summary>
        /// Comment character
        /// </summary>
        public const char COMMENT_CHAR = '#';

        /// <summary>
        /// True if the line text has been updated (or is new)
        /// </summary>
        /// <remarks>
        /// Also set to true if the constructor that accepts an MSGFPlusParameter is called,
        /// since this is a new parameter that needs to be included when when re-writing the parameter file</remarks>
        public bool LineUpdated { get; private set; }

        /// <summary>
        /// Parameter info
        /// </summary>
        public MSGFPlusParameter ParamInfo { get; private set; }

        /// <summary>
        /// This text is used when commenting out a line when re-writing the parameter file (e.g. due to an obsolete parameter)
        /// </summary>
        private string PrefixToAdd { get; set; } = string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramFileLine"></param>
        public MSGFPlusKeyValueParamFileLine(KeyValueParamFileLine paramFileLine) : base(paramFileLine.LineNumber, paramFileLine.Text)
        {
        }

        /// <summary>
        /// Constructor for appending a new parameter to the parameter file
        /// </summary>
        /// <param name="paramInfo"></param>
        public MSGFPlusKeyValueParamFileLine(MSGFPlusParameter paramInfo) : base(0, paramInfo.GetKeyValueParamNoComment())
        {
            StoreParameter(paramInfo);
            LineUpdated = true;
        }

        /// <summary>
        /// Change this line to a comment when re-writing the parameter file
        /// </summary>
        /// <param name="commentReason"></param>
        public void ChangeLineToComment(string commentReason = "")
        {
            if (string.IsNullOrWhiteSpace(commentReason))
            {
                PrefixToAdd = string.Format("{0} ", COMMENT_CHAR);
            }
            else
            {
                PrefixToAdd = string.Format("{0} {1}: ", COMMENT_CHAR, commentReason);
            }

            UpdateTextLine();
        }

        /// <summary>
        /// Replace the parameter associated with this parameter file line
        /// </summary>
        /// <param name="paramInfo"></param>
        public void ReplaceParameter(MSGFPlusParameter paramInfo)
        {
            StoreParameter(paramInfo);
            UpdateTextLine();
        }

        /// <summary>
        /// Associate a parameter with this parameter file line
        /// </summary>
        /// <param name="paramInfo"></param>
        public void StoreParameter(MSGFPlusParameter paramInfo)
        {
            StoreParameter(paramInfo.ParameterName, paramInfo.Value);
            ParamInfo = paramInfo;
        }

        /// <summary>
        /// Update the value for this line's parameter
        /// </summary>
        /// <param name="valueOverride"></param>
        public void UpdateParamValue(string valueOverride)
        {
            ParamInfo.UpdateValue(valueOverride);
            UpdateTextLine();
        }

        /// <summary>
        /// Update the Text for this line using the current parameter name, value, and optionally comment
        /// </summary>
        private void UpdateTextLine()
        {
            var prefix = PrefixToAdd ?? string.Empty;

            var keyValueParam = string.Format("{0}={1}", ParamInfo.ParameterName, ParamInfo.Value);

            var comment = ParamInfo.CommentWithPrefix ?? string.Empty;

            Text = prefix + keyValueParam + comment;

            LineUpdated = true;
        }
    }
}
