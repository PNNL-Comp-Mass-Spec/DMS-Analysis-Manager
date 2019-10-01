using System;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManager_Ape_PlugIn
{
    public class clsApeAMOperations : EventNotifier
    {

        #region Member Variables

        protected IJobParams mJobParams;

        protected IMgrParams mMgrParams;

        private string mErrorMessage = string.Empty;

        #endregion

        #region "Properties"

        public string ErrorMessage => mErrorMessage;

        #endregion

        #region Constructors

        public clsApeAMOperations(IJobParams jobParams, IMgrParams mgrParams)
        {
            mJobParams = jobParams;
            mMgrParams = mgrParams;
        }

        #endregion

        /// <summary>
        /// Run a list of Ape operations
        /// </summary>
        /// <param name="apeOperations"></param>
        /// <returns></returns>
        public bool RunApeOperations(string apeOperations)
        {
            var ok = false;
            foreach (var apeOperation in apeOperations.Split(','))
            {
                ok = RunApeOperation(apeOperation.Trim());
                if (!ok) break;
            }
            return ok;
        }

        #region Ape Operations

        /// <summary>
        /// Run defined Ape operation(s)
        /// </summary>
        /// <param name="apeOperation"></param>
        /// <returns></returns>
        private bool RunApeOperation(string apeOperation)
        {

            if (apeOperation.Equals("RunWorkflow", StringComparison.OrdinalIgnoreCase))
            {
                var apeWfObj = new clsApeAMRunWorkflow(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeWfObj);

                var success = apeWfObj.RunWorkflow();

                if (!success)
                    mErrorMessage = "Error running apeWorkflow: " + apeWfObj.ErrorMessage;

                return success;
            }

            if (apeOperation.Equals("GetImprovResults", StringComparison.OrdinalIgnoreCase))
            {
                var apeImpObj = new clsApeAMGetImprovResults(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeImpObj);

                var success = apeImpObj.GetImprovResults(mJobParams.GetParam("DataPackageID"));

                if (!success)
                    mErrorMessage = "Error getting ImprovResults: " + apeImpObj.ErrorMessage;

                return success;
            }

            if (apeOperation.Equals("GetQRollupResults", StringComparison.OrdinalIgnoreCase))
            {
                var apeQImpObj = new clsApeAMGetQRollupResults(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeQImpObj);

                var success = apeQImpObj.GetQRollupResults(mJobParams.GetParam("DataPackageID"));

                if (!success)
                    mErrorMessage = "Error obtaining QRollup Results: " + apeQImpObj.ErrorMessage;

                return success;
            }

            if (apeOperation.Equals("GetViperResults", StringComparison.OrdinalIgnoreCase))
            {
                var apeVImpObj = new clsApeAMGetViperResults(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeVImpObj);

                var success = apeVImpObj.GetQRollupResults(mJobParams.GetParam("DataPackageID"));

                if (!success)
                    mErrorMessage = "Error obtaining VIPER results: " + apeVImpObj.ErrorMessage;

                return success;
            }

            mErrorMessage = "Ape Operation: " + apeOperation + "not recognized";
            return false;

        }

        private void RegisterEventsCustomProgressHandler(EventNotifier sourceClass)
        {
            sourceClass.DebugEvent += ApeProgressChanged;
            sourceClass.StatusEvent += OnStatusEvent;
            sourceClass.ErrorEvent += OnErrorEvent;
            sourceClass.WarningEvent += OnWarningEvent;
            sourceClass.ProgressUpdate += OnProgressUpdate;
        }

        void ApeProgressChanged(string message)
        {

            // Update the step tool progress
            // However, Ape routinely reports progress of 0% or 100% at the start and end of certain subtasks, so ignore those values
            // if (e.percentComplete > 0 && e.percentComplete < 100)
            // mProgress = PROGRESS_PCT_APE_START + (PROGRESS_PCT_APE_DONE - PROGRESS_PCT_APE_START) * e.percentComplete / 100.0F;

            // if (!string.IsNullOrEmpty(e.taskDescription))
            // mCurrentApeTask = e.taskDescription;

            // if (System.DateTime.UtcNow.Subtract(mLastStatusUpdateTime).TotalSeconds >= 10)
            // {
            //    mLastStatusUpdateTime = System.DateTime.UtcNow;
            //    mStatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, mProgress);
            // }
        }

        #endregion


    }
}
