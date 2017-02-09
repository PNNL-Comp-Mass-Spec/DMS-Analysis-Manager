
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    public interface IToolRunner
    {

        //*********************************************************************************************************
        //Insert general class description here
        //*********************************************************************************************************

        #region "Properties"
        int EvalCode { get; }

        string EvalMessage { get; }

        string ResFolderName { get; }
        // Explanation of what happened to last operation this class performed
        // Used to report error messages

        string Message { get; }

        bool NeedToAbortProcessing { get; }
        // the state of completion of the job (as a percentage)
        #endregion
        float Progress { get; }

        #region "Methods"

        void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsSummaryFile summaryFile, clsMyEMSLUtilities myEMSLUtilities);
        CloseOutType RunTool();

        #endregion

    }

}