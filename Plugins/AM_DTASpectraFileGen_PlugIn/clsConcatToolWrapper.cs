//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2005, Battelle Memorial Institute
// Started 11/03/2005
//
//*********************************************************************************************************

using System;
using System.Threading;
using AnalysisManagerBase;
using FileConcatenator;

namespace DTASpectraFileGen
{
    /// <summary>
    /// Provides a wrapper around Ken Auberry's file concatenator dll to simplify use
    /// Requires FileConcatenator.dll to be referenced in project
    /// </summary>
    public class clsConcatToolWrapper
    {
        #region "Enums"

        public enum ConcatFileTypes
        {
            CONCAT_DTA,
            CONCAT_OUT,
            CONCAT_ALL
        }

        #endregion

        #region "Module variables"

        private bool m_CatInProgress = false;
        private IConcatenateFiles m_CatTools;
        private string m_ErrMsg = "";
        private string m_DataPath = "";
        private float m_Progress = 0.0f;        //Percent complete, 0-100

        #endregion

        #region "Properties"

        public float Progress
        {
            get { return m_Progress; }
        }

        public string ErrMsg
        {
            get { return m_ErrMsg; }
        }

        public string DataPath
        {
            get { return m_DataPath; }
            set { m_DataPath = value; }
        }

        #endregion

        #region "Public Methods"

        public clsConcatToolWrapper(string DataPath)
        {
            m_DataPath = DataPath;
        }

        public bool ConcatenateFiles(ConcatFileTypes FileType, string RootFileName)
        {
            return ConcatenateFiles(FileType, RootFileName, false);
        }

        public bool ConcatenateFiles(ConcatFileTypes FileType, string RootFileName, bool blnDeleteSourceFilesWhenConcatenating)
        {
            try
            {
                //Perform the concatenation
                m_CatTools = new clsConcatenateFiles(m_DataPath, RootFileName);
                m_CatTools.DeleteSourceFilesWhenConcatenating = blnDeleteSourceFilesWhenConcatenating;
                m_CatTools.ErrorNotification += m_CatTools_ErrorNotification;
                m_CatTools.EndTask += m_CatTools_EndingTask;
                m_CatTools.Progress += m_CatTools_Progress;

                m_CatInProgress = true;

                //Call the dll based on the concatenation type
                switch (FileType)
                {
                    case ConcatFileTypes.CONCAT_ALL:
                        m_CatTools.MakeCattedDTAsAndOUTs();
                        break;
                    case ConcatFileTypes.CONCAT_DTA:
                        m_CatTools.MakeCattedDTAsOnly();
                        break;
                    case ConcatFileTypes.CONCAT_OUT:
                        m_CatTools.MakeCattedOUTsOnly();
                        break;
                    default:
                        //Shouldn't ever get here
                        m_ErrMsg = "Invalid concatenation selection: " + FileType.ToString();
                        return false;
                }

                //Loop until the concatenation finishes
                while (m_CatInProgress)
                {
                    Thread.Sleep(1000);
                }

                //Concatenation must have finished successfully, so exit
                return true;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Exception while concatenating files: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                return false;
            }
        }

        #endregion

        #region "Private methods"

        private void m_CatTools_ErrorNotification(string errorMessage)
        {
            m_CatInProgress = false;
            m_ErrMsg = errorMessage;
        }

        //Private Sub m_CatTools_StartingTask(ByVal taskIdentString As String) Handles m_CatTools.StartingTask
        //	m_CatInProgress = True
        //End Sub

        private void m_CatTools_EndingTask()
        {
            m_CatInProgress = false;
        }

        private void m_CatTools_Progress(double fractionDone)
        {
            m_Progress = (float)(100.0 * fractionDone);
        }

        #endregion
    }
}
