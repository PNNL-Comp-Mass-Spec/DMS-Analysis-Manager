
using System;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Specialized handler for "file not found" exception
    /// </summary>
    public class AMFileNotFoundException : ApplicationException
    {

        #region "Properties"

        public string FileName { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="FileName">Name of file being processed when exception occurred</param>
        /// <param name="Message">Message to be returned in exception</param>
        /// <remarks></remarks>
        public AMFileNotFoundException(string FileName, string Message) : base(Message)
        {
            this.FileName = FileName;

        }

        #endregion

    }

    /// <summary>
    /// Specialized handler for "folder not found" exception
    /// </summary>
    public class AMFolderNotFoundException : ApplicationException
    {

        #region "Properties"

        public string FolderName { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="FolderName">Name of unfound folder</param>
        /// <param name="Message">Message for exception to return</param>
        /// <remarks></remarks>
        public AMFolderNotFoundException(string FolderName, string Message) : base(Message)
        {
            this.FolderName = FolderName;

        }
        #endregion

    }

    /// <summary>
    /// Specialized handler for file deletion exception after multiple retries
    /// </summary>
    public class AMFileNotDeletedAfterRetryException : ApplicationException
    {

        #region "Enums"
        public enum RetryExceptionType
        {
            IO_Exception,
            Unauthorized_Access_Exception
        }
        #endregion

        #region "Properties"

        public string FileName { get; }

        public RetryExceptionType ExcType { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="FileName">Name of file causing exception</param>
        /// <param name="ExceptionType">Exception type</param>
        /// <param name="Message">Message to be returned by exception</param>
        /// <remarks></remarks>
        public AMFileNotDeletedAfterRetryException(string FileName, RetryExceptionType ExceptionType, string Message) : base(Message)
        {
            this.FileName = FileName;
            ExcType = ExceptionType;

        }
        #endregion

    }

    /// <summary>
    /// Specialized handler for file deletion exception
    /// </summary>
    public class AMFileNotDeletedException : ApplicationException
    {

        #region "Properties"

        public string FileName { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="FileName">Name of file causing exception</param>
        /// <param name="Message">Message to be returned by exception</param>
        /// <remarks></remarks>
        public AMFileNotDeletedException(string FileName, string Message) : base(Message)
        {
            this.FileName = FileName;

        }
        #endregion

    }

}