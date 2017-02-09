
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

    public class AMFileNotFoundException : ApplicationException
    {

        //*********************************************************************************************************
        //Specialized handler for "file not found" exception
        //*********************************************************************************************************

        #region "Module variables"
        #endregion
        private string m_FileName;

        #region "Properties"
        public string FileName
        {
            get { return m_FileName; }
        }
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
            m_FileName = FileName;

        }
        #endregion

    }


    public class AMFolderNotFoundException : ApplicationException
    {

        //*********************************************************************************************************
        //Specialized handler for "folder not found" exception
        //*********************************************************************************************************

        #region "Module variables"
        #endregion
        private string m_FolderName;

        #region "Properties"
        public string FolderName
        {
            get { return m_FolderName; }
        }
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
            m_FolderName = FolderName;

        }
        #endregion

    }

    public class AMFileNotDeletedAfterRetryException : ApplicationException
    {

        //*********************************************************************************************************
        //Specialized handler for file deletion exception after multiple retries
        //*********************************************************************************************************

        #region "Enums"
        public enum RetryExceptionType
        {
            IO_Exception,
            Unauthorized_Access_Exception
        }
        #endregion

        #region "Module variables"
        private string m_FileName;
        #endregion
        private RetryExceptionType m_ExceptionType;

        #region "Properties"
        public string FileName
        {
            get { return m_FileName; }
        }

        public RetryExceptionType ExcType
        {
            get { return m_ExceptionType; }
        }
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
            m_FileName = FileName;
            m_ExceptionType = ExceptionType;

        }
        #endregion

    }

    public class AMFileNotDeletedException : ApplicationException
    {

        //*********************************************************************************************************
        //Specialized handler for file deletion exception
        //*********************************************************************************************************

        #region "Module variables"
        #endregion
        private string m_FileName;

        #region "Properties"
        public string FileName
        {
            get { return m_FileName; }
        }
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
            m_FileName = FileName;

        }
        #endregion

    }
    
}