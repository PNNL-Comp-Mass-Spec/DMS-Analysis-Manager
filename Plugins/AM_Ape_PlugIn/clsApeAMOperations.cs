using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    public class clsApeAMOperations
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
            bool blnSuccess;

            // Note: case statements must be lowercase
            switch (apeOperation.ToLower())
            {
                case "runworkflow":
                    var apeWfObj = new clsApeAMRunWorkflow(mJobParams, mMgrParams);

                    // Attach the progress event handler
                    apeWfObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeWfObj.RunWorkflow();

                    if (!blnSuccess)
                        mErrorMessage = "Error running apeWorkflow: " + apeWfObj.ErrorMessage;

                    break;

                case "getimprovresults":
                    var apeImpObj = new clsApeAMGetImprovResults(mJobParams, mMgrParams);

                    // Attach the progress event handler
                    apeImpObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeImpObj.GetImprovResults(mJobParams.GetParam("DataPackageID"));

                    if (!blnSuccess)
                        mErrorMessage = "Error getting ImprovResults: " + apeImpObj.ErrorMessage;

                    break;

                case "getqrollupresults":
                    var apeQImpObj = new clsApeAMGetQRollupResults(mJobParams, mMgrParams);

                    // Attach the progress event handler
                    apeQImpObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeQImpObj.GetQRollupResults(mJobParams.GetParam("DataPackageID"));

                    if (!blnSuccess)
                        mErrorMessage = "Error obtaining QRollup Results: " + apeQImpObj.ErrorMessage;

                    break;

                case "getviperresults":
                    var apeVImpObj = new clsApeAMGetViperResults(mJobParams, mMgrParams);

                    // Attach the progress event handler
                    apeVImpObj.ProgressChanged += ApeProgressChanged;

                    blnSuccess = apeVImpObj.GetQRollupResults(mJobParams.GetParam("DataPackageID"));

                    if (!blnSuccess)
                        mErrorMessage = "Error obtaining VIPER results: " + apeVImpObj.ErrorMessage;

                    break;

                default:
                    blnSuccess = false;
                    mErrorMessage = "Ape Operation: " + apeOperation + "not recognized";
                    break;
            }
            return blnSuccess;
        }

        void ApeProgressChanged(object sender, clsApeAMBase.ProgressChangedEventArgs e)
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
