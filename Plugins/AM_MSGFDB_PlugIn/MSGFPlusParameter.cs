using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace AnalysisManagerMSGFDBPlugIn
{
    public class MSGFPlusParameter : PRISM.EventNotifier
    {
        /// <summary>
        /// Name of the parameter in an MS-GF+ parameter file
        /// </summary>
        public string ParameterName { get; }

        /// <summary>
        /// Command line argument for this parameter
        /// </summary>
        /// <remarks>Empty string if not applicable</remarks>
        public string CommandLineArg { get; }

        /// <summary>
        /// Comment text associated with this parameter (text to be included on the same line as this parameter's Key=Value definition the param file)
        /// </summary>
        /// <remarks>Does not include the # sign</remarks>
        public string Comment { get; private set; }

        /// <summary>
        /// Get the comment text to include on the same line as this parameter's Key=Value definition the param file
        /// Includes both WhiteSpaceBeforeComment and Comment
        /// </summary>
        public string CommentWithPrefix
        {
            get
            {
                if (string.IsNullOrWhiteSpace(Comment))
                    return string.Empty;

                string commentText;

                if (string.IsNullOrWhiteSpace(WhiteSpaceBeforeComment))
                {
                    commentText = Comment;
                }
                else
                {
                    commentText = WhiteSpaceBeforeComment + Comment;
                }

                var commentChar = MSGFPlusKeyValueParamFileLine.COMMENT_CHAR.ToString();

                if (commentText.Trim().StartsWith(commentChar))
                    return commentText;

                return " " + commentChar + " " + commentText;
            }
        }

        /// <summary>
        /// Parameter value
        /// </summary>
        public string Value { get; private set; }

        /// <summary>
        /// When true, the text stored in the Value property will not be changed via a call to UpdateValue
        /// </summary>
        /// <remarks>
        /// When reading the MS-GF+ (or MzRefinery) parameter file, if a value ends with an exclamation mark,
        /// the exclamation mark will be removed by the Analysis Manager and ValueLocked will be set to true
        /// </remarks>
        public bool ValueLocked { get; set; }

        /// <summary>
        /// Alternate names used for this parameter in an MS-GF+ parameter file
        /// </summary>
        /// <remarks>Case-insensitive names</remarks>
        public SortedSet<string> SynonymList { get; }

        /// <summary>
        /// Whitespace between the parameter value and the comment, including the # sign
        /// </summary>
        public string WhiteSpaceBeforeComment { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramName">Parameter name</param>
        /// <param name="commandLineArgName">Command line argument</param>
        /// <param name="paramNameSynonym">Synonym for the parameter name (add additional synonyms with AddSynonym)</param>
        public MSGFPlusParameter(string paramName, string commandLineArgName, string paramNameSynonym = "")
        {
            ParameterName = paramName ?? string.Empty;
            CommandLineArg = commandLineArgName ?? string.Empty;
            SynonymList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrWhiteSpace(paramNameSynonym))
            {
                SynonymList.Add(paramNameSynonym);
            }

            Comment = string.Empty;
            WhiteSpaceBeforeComment = string.Empty;
            Value = string.Empty;
        }

        /// <summary>
        /// Add a synonym for the parameter name
        /// </summary>
        /// <param name="synonymName">Synonym name</param>
        public void AddSynonym(string synonymName)
        {
            if (string.Equals(ParameterName, synonymName, StringComparison.OrdinalIgnoreCase))
                return;

            if (SynonymList.Contains(synonymName))
                return;

            SynonymList.Add(synonymName);
        }

        /// <summary>
        /// Get the Key=Value text for this parameter
        /// </summary>
        public string GetKeyValueParamNoComment()
        {
            if (string.IsNullOrWhiteSpace(ParameterName))
                return string.Empty;

            return string.Format("{0}={1}", ParameterName, Value ?? string.Empty);
        }

        /// <summary>
        /// Check whether this parameter has this synonym
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        public bool HasSynonym(string parameterName)
        {
            return SynonymList.Contains(parameterName);
        }

        /// <summary>
        /// Update the comment for this parameter
        /// </summary>
        /// <remarks>This comment is the text to be included on the same line as this parameter's Key=Value definition the param file</remarks>
        /// <param name="comment">Comment</param>
        /// <param name="whitespaceBeforeComment">Whitespace to add before the comment</param>
        public void UpdateComment(string comment, string whitespaceBeforeComment)
        {
            Comment = comment ?? string.Empty;

            if (string.IsNullOrWhiteSpace(whitespaceBeforeComment))
            {
                var commentChar = MSGFPlusKeyValueParamFileLine.COMMENT_CHAR.ToString();

                if (Comment.Trim().StartsWith(commentChar))
                    WhiteSpaceBeforeComment = string.Empty;
                else
                    WhiteSpaceBeforeComment = " " + commentChar + " ";
            }
            else
            {
                WhiteSpaceBeforeComment = whitespaceBeforeComment;
            }
        }

        /// <summary>
        /// Update the value for this parameter
        /// </summary>
        /// <remarks>
        /// If value ends with an exclamation mark, the exclamation mark will be removed and ValueLocked will be set to true
        /// This method will return true in this case, since the value was likely changed (but cannot be changed again)
        /// </remarks>
        /// <param name="value">New value</param>
        /// <param name="callerName">Calling method name</param>
        /// <returns>
        /// True if the value was updated or the value is locked, but the new value is the same
        /// False if the value is locked and the new value is different
        /// </returns>
        public bool UpdateValue(string value, [CallerMemberName] string callerName = "")
        {
            if (ValueLocked)
            {
                if (value?.Equals(Value) == true)
                    return true;

                OnWarningEvent("Not changing locked value for {0} from '{1}' to '{2}'; calling method: {3}", ParameterName, Value ?? string.Empty, value ?? string.Empty, callerName);

                return false;
            }

            if (value?.EndsWith("!") == true)
            {
                Value = value.TrimEnd('!');
                ValueLocked = true;
                return true;
            }

            Value = value ?? string.Empty;
            return true;
        }

        /// <summary>
        /// Create a copy of this parameter, by default not copying the value
        /// </summary>
        /// <param name="copyValue">True to also copy the value associated with this parameter</param>
        /// <returns>New MSGFPlusParameter</returns>
        public MSGFPlusParameter Clone(bool copyValue = false)
        {
            return Clone(copyValue, string.Empty);
        }

        /// <summary>
        /// Create a copy of this parameter and associate a new value with the cloned parameter
        /// </summary>
        /// <param name="newValue">New value</param>
        /// <returns>New MSGFPlusParameter</returns>
        public MSGFPlusParameter Clone(string newValue)
        {
            return Clone(false, newValue);
        }

        /// <summary>
        /// Create a copy of this parameter, by default not copying the value
        /// </summary>
        /// <param name="copyValue">True to also copy the value associated with this parameter</param>
        /// <param name="newValue">New value to associate with the cloned parameter</param>
        /// <returns>New MSGFPlusParameter</returns>
        private MSGFPlusParameter Clone(bool copyValue, string newValue)
        {
            var parameterCopy = new MSGFPlusParameter(ParameterName, CommandLineArg);

            foreach (var synonym in SynonymList)
            {
                parameterCopy.AddSynonym(synonym);
            }

            parameterCopy.UpdateComment(Comment, WhiteSpaceBeforeComment);

            parameterCopy.UpdateValue(copyValue ? Value : newValue);

            if (!parameterCopy.ValueLocked)
            {
                parameterCopy.ValueLocked = ValueLocked;
            }

            return parameterCopy;
        }

        /// <summary>
        /// Show the parameter name and value
        /// </summary>
        public override string ToString()
        {
            var lockedFlag = ValueLocked ? " (Locked)" : string.Empty;
            return string.Format("{0}: {1}{2}", ParameterName, Value, lockedFlag);
        }
    }
}
