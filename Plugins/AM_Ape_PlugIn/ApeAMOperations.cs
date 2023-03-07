using System;
using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;
using PRISM;
using PRISM.Logging;

namespace AnalysisManager_Ape_PlugIn
{
    public class ApeAMOperations : EventNotifier
    {
        private readonly IJobParams mJobParams;

        private readonly IMgrParams mMgrParams;

        private DateTime mLastProgressTime = DateTime.UtcNow;

        public string ErrorMessage { get; private set; } = string.Empty;

        public ApeAMOperations(IJobParams jobParams, IMgrParams mgrParams)
        {
            mJobParams = jobParams;
            mMgrParams = mgrParams;
        }

        /// <summary>
        /// Run a list of Ape operations
        /// </summary>
        /// <param name="apeOperations"></param>
        public bool RunApeOperations(string apeOperations)
        {
            var ok = false;

            foreach (var apeOperation in apeOperations.Split(','))
            {
                ok = RunApeOperation(apeOperation.Trim());

                if (!ok)
                    break;
            }
            return ok;
        }

        /// <summary>
        /// Run defined Ape operation(s)
        /// </summary>
        /// <param name="apeOperation"></param>
        private bool RunApeOperation(string apeOperation)
        {
            if (apeOperation.Equals("RunWorkflow", StringComparison.OrdinalIgnoreCase))
            {
                var apeWfObj = new ApeAMRunWorkflow(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeWfObj);

                var success = apeWfObj.RunWorkflow();

                if (!success)
                {
                    ErrorMessage = "Error running apeWorkflow: " + apeWfObj.ErrorMessage;
                }

                return success;
            }

            if (apeOperation.Equals("GetImprovResults", StringComparison.OrdinalIgnoreCase))
            {
                var apeImpObj = new ApeAMGetImprovResults(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeImpObj);

                var success = apeImpObj.GetImprovResults();

                if (!success)
                    ErrorMessage = "Error getting ImprovResults: " + apeImpObj.ErrorMessage;

                return success;
            }

            if (apeOperation.Equals("GetQRollupResults", StringComparison.OrdinalIgnoreCase))
            {
                var apeQImpObj = new ApeAMGetQRollupResults(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeQImpObj);

                var success = apeQImpObj.GetQRollupResults();

                if (!success)
                    ErrorMessage = "Error obtaining QRollup Results: " + apeQImpObj.ErrorMessage;

                return success;
            }

            if (apeOperation.Equals("GetViperResults", StringComparison.OrdinalIgnoreCase))
            {
                var apeVImpObj = new ApeAMGetViperResults(mJobParams, mMgrParams);

                // Attach the event handlers
                RegisterEventsCustomProgressHandler(apeVImpObj);

                var success = apeVImpObj.GetQRollupResults();

                if (!success)
                    ErrorMessage = "Error obtaining VIPER results: " + apeVImpObj.ErrorMessage;

                return success;
            }

            ErrorMessage = "Ape Operation: " + apeOperation + "not recognized";
            return false;
        }

        private void RegisterEventsCustomProgressHandler(IEventNotifier sourceClass)
        {
            sourceClass.DebugEvent += ApeProgressChanged;
            sourceClass.StatusEvent += OnStatusEvent;
            sourceClass.ErrorEvent += OnErrorEvent;
            sourceClass.WarningEvent += OnWarningEvent;
            sourceClass.ProgressUpdate += OnProgressUpdate;
        }

        private void ApeProgressChanged(string message)
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
            //    mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL, mProgress);
            // }

            if (DateTime.UtcNow.Subtract(mLastProgressTime).TotalSeconds < 9)
                return;

            mLastProgressTime = DateTime.UtcNow;
            ConsoleMsgUtils.ShowDebug(message);
        }
    }
}
