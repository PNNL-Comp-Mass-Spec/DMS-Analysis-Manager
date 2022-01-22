
using System;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
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
        /// <summary>
        /// Path to the file that was not found
        /// </summary>
        public string FilePath { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filePath">Path of file not found</param>
        /// <param name="errorMessage">Exception message</param>
        public AMFileNotFoundException(string filePath, string errorMessage) : base(errorMessage)
        {
            FilePath = filePath;
        }
    }

    /// <summary>
    /// Specialized handler for file deletion exception after multiple retries
    /// </summary>
    public class AMFileNotDeletedAfterRetryException : ApplicationException
    {
        /// <summary>
        /// Enum for reason that file could not be deleted
        /// </summary>
        public enum RetryExceptionType
        {
            /// <summary>
            /// I/O exception
            /// </summary>
            IO_Exception,

            /// <summary>
            /// Unauthorized access exception
            /// </summary>
            Unauthorized_Access_Exception
        }

        /// <summary>
        /// Path to the file that could not be deleted
        /// </summary>
        public string FilePath { get; }

        /// <summary>
        /// Reason that file could not be deleted
        /// </summary>
        public RetryExceptionType ExcType { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filePath">Path of file that could not be deleted</param>
        /// <param name="ExceptionType">Exception type</param>
        /// <param name="errorMessage">Exception message</param>
        public AMFileNotDeletedAfterRetryException(string filePath, RetryExceptionType ExceptionType, string errorMessage) : base(errorMessage)
        {
            FilePath = filePath;
            ExcType = ExceptionType;
        }
    }

    /// <summary>
    /// Specialized handler for file deletion exception
    /// </summary>
    public class AMFileNotDeletedException : ApplicationException
    {
        /// <summary>
        /// Path to the file that could not be deleted
        /// </summary>
        public string FilePath { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filePath">Path of file that could not be deleted</param>
        /// <param name="errorMessage">Exception message</param>
        public AMFileNotDeletedException(string filePath, string errorMessage) : base(errorMessage)
        {
            FilePath = filePath;
        }
    }
}