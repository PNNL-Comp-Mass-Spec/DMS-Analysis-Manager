//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2005, Battelle Memorial Institute
// Started 11/03/2005
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;
using FileConcatenator;

namespace DTASpectraFileGen
{
    /// <summary>
    /// Provides a wrapper around FileConcatenator.dll to simplify use
    /// Requires FileConcatenator.dll to be referenced in project
    /// </summary>
    public class ConcatToolWrapper
    {
        public enum ConcatFileTypes
        {
            CONCAT_DTA,
            CONCAT_OUT,
            CONCAT_ALL
        }

        private bool mCatInProgress;

        private IConcatenateFiles mCatTools;

        public float Progress { get; private set; }

        public string ErrMsg { get; private set; }

        public string WorkingDirectory { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workingDirectory">Working directory path</param>
        public ConcatToolWrapper(string workingDirectory)
        {
            WorkingDirectory = workingDirectory;
            ErrMsg = string.Empty;
        }

        public bool ConcatenateFiles(ConcatFileTypes fileType, string rootFileName)
        {
            return ConcatenateFiles(fileType, rootFileName, false);
        }

        public bool ConcatenateFiles(ConcatFileTypes fileType, string rootFileName, bool deleteSourceFilesWhenConcatenating)
        {
            try
            {
                // Perform the concatenation
                mCatTools = new clsConcatenateFiles(WorkingDirectory, rootFileName)
                {
                    DeleteSourceFilesWhenConcatenating = deleteSourceFilesWhenConcatenating
                };

                mCatTools.ErrorNotification += CatTools_ErrorNotification;
                mCatTools.EndTask += CatTools_EndingTask;
                mCatTools.Progress += CatTools_Progress;

                mCatInProgress = true;

                // Call the dll based on the concatenation type
                switch (fileType)
                {
                    case ConcatFileTypes.CONCAT_ALL:
                        mCatTools.MakeCattedDTAsAndOUTs();
                        break;
                    case ConcatFileTypes.CONCAT_DTA:
                        mCatTools.MakeCattedDTAsOnly();
                        break;
                    case ConcatFileTypes.CONCAT_OUT:
                        mCatTools.MakeCattedOUTsOnly();
                        break;
                    default:
                        // Shouldn't ever get here
                        ErrMsg = "Invalid concatenation selection: " + fileType;
                        return false;
                }

                // Loop until the concatenation finishes
                while (mCatInProgress)
                {
                    Global.IdleLoop(1);
                }

                // Concatenation must have finished successfully, so exit
                return true;
            }
            catch (Exception ex)
            {
                ErrMsg = "Exception while concatenating files: " + ex.Message + "; " + Global.GetExceptionStackTrace(ex);
                return false;
            }
        }

        private void CatTools_ErrorNotification(string errorMessage)
        {
            mCatInProgress = false;
            ErrMsg = errorMessage;
        }

        private void CatTools_EndingTask()
        {
            mCatInProgress = false;
        }

        private void CatTools_Progress(double fractionDone)
        {
            Progress = (float)(100.0 * fractionDone);
        }
    }
}
