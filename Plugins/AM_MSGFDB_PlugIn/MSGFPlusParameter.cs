using System;
using System.Collections.Generic;

namespace AnalysisManagerMSGFDBPlugIn
{
    public class MSGFPlusParameter
    {
        /// <summary>
        /// Name of the parameter in an MSGF+ parameter file
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
        /// Alternate names used for this parameter in an MSGF+ parameter file
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
        /// <param name="synonymName"></param>
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
        /// <returns></returns>
        public string GetKeyValueParamNoComment()
        {
            if (string.IsNullOrWhiteSpace(ParameterName))
                return string.Empty;

            return string.Format("{0}={1}", ParameterName, Value ?? string.Empty);
        }

        /// <summary>
        /// Check whether this parameter has this synonym
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        public bool HasSynonym(string parameterName)
        {
            return SynonymList.Contains(parameterName);
        }

        /// <summary>
        /// Update the comment for this parameter
        /// </summary>
        /// <param name="comment"></param>
        /// <param name="whitespaceBeforeComment"></param>
        /// <remarks>This comment is the text to be included on the same line as this parameter's Key=Value definition the param file</remarks>
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
        /// <param name="value"></param>
        public void UpdateValue(string value)
        {
            Value = value ?? string.Empty;
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

            return parameterCopy;
        }


        /// <summary>
        /// Return the text of the line from the parameter file (including the comment, if any)
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ParameterName + ": " + Value;
        }

    }
}
