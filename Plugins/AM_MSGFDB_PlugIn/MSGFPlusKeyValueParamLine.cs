using PRISM.AppSettings;

namespace AnalysisManagerMSGFDBPlugIn
{
    internal class MSGFPlusKeyValueParamFileLine : KeyValueParamFileLine
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
        /// since this is a new parameter that needs to be included when re-writing the parameter file
        /// </remarks>
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
        /// Constructor for appending a new line to the parameter file
        /// </summary>
        /// <remarks>If the line has a parameter, use the constructor that takes a MSGFPlusParameter object instead</remarks>
        /// <param name="paramFileLine">Key/value parameter file line</param>
        /// <param name="isAdditionalLine">Set this to true if this is an additional parameter line (or blank line or comment line) that needs to be appended</param>
        public MSGFPlusKeyValueParamFileLine(
            KeyValueParamFileLine paramFileLine,
            bool isAdditionalLine = false) : base(paramFileLine)
        {
            LineUpdated = isAdditionalLine;
        }

        /// <summary>
        /// Constructor for appending a new parameter to the parameter file
        /// </summary>
        /// <param name="paramInfo">MS-GF+ parameter</param>
        public MSGFPlusKeyValueParamFileLine(MSGFPlusParameter paramInfo) : base(0, paramInfo.GetKeyValueParamNoComment())
        {
            StoreParameter(paramInfo);
            LineUpdated = true;
        }

        /// <summary>
        /// Change this line to a comment when re-writing the parameter file
        /// </summary>
        /// <param name="commentReason">Comment reason</param>
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
        /// <param name="paramInfo">MS-GF+ parameter</param>
        public void ReplaceParameter(MSGFPlusParameter paramInfo)
        {
            StoreParameter(paramInfo);
            UpdateTextLine();
        }

        /// <summary>
        /// Associate a parameter with this parameter file line
        /// </summary>
        /// <param name="paramInfo">MS-GF+ parameter</param>
        public void StoreParameter(MSGFPlusParameter paramInfo)
        {
            StoreParameter(paramInfo.ParameterName, paramInfo.Value);
            ParamInfo = paramInfo;
        }

        /// <summary>
        /// Update the value for this line's parameter
        /// </summary>
        /// <param name="valueOverride">New value to use</param>
        /// <param name="includeOriginalAsComment">If true, include the original value as a comment</param>
        public void UpdateParamValue(string valueOverride, bool includeOriginalAsComment = false)
        {
            var originalValue = string.Copy(ParamInfo.GetKeyValueParamNoComment());
            var updateAllowed = ParamInfo.UpdateValue(valueOverride);

            if (updateAllowed)
            {
                // Also update the value tracked by base.ParamValue
                UpdateValue(valueOverride);
            }

            if (includeOriginalAsComment)
            {
                const string whitespaceBeforeComment = "        # ";
                ParamInfo.UpdateComment(originalValue, whitespaceBeforeComment);
            }

            UpdateTextLine();
        }

        /// <summary>
        /// Update the Text for this line using the current parameter name, value, and optionally comment
        /// </summary>
        private void UpdateTextLine()
        {
            LineUpdated = true;

            var prefix = PrefixToAdd ?? string.Empty;

            if (ParamInfo == null || string.IsNullOrWhiteSpace(ParamInfo.ParameterName))
            {
                Text = prefix + Text;
                return;
            }

            var keyValueParam = ParamInfo.GetKeyValueParamNoComment();

            var comment = ParamInfo.CommentWithPrefix ?? string.Empty;

            Text = prefix + keyValueParam + comment;
        }
    }
}
