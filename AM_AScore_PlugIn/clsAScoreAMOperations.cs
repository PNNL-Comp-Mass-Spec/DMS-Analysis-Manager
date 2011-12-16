using System;
using AnalysisManagerBase;

namespace AnalysisManager_AScore_PlugIn
{
    public class clsAScoreAMOperations
    {

        #region Member Variables

        protected IJobParams m_jobParams;

        protected IMgrParams m_mgrParams;

        #endregion

        #region Constructors

        public clsAScoreAMOperations(IJobParams jobParms, IMgrParams mgrParms) {
            m_jobParams = jobParms;
            m_mgrParams = mgrParms;
        }

        #endregion

        /// <summary>
        /// Run a list of AScore operations
        /// </summary>
        /// <param name="mageOperations"></param>
        /// <returns></returns>
        public bool RunAScoreOperations(string ascoreOperations) {
            bool ok = false;
            foreach (string ascoreOperation in ascoreOperations.Split(',')) {
                ok = RunAScoreOperation(ascoreOperation.Trim());
                if (!ok) break;
            }
            return ok;
        }

        #region AScore Operations

        /// <summary>
        /// Run defined AScore operation(s)
        /// </summary>
        /// <param name="ascoreOperation"></param>
        /// <returns></returns>
        private bool RunAScoreOperation(string ascoreOperation)
        {
            bool blnSuccess = false;

            // Note: case statements must be lowercase
            switch (ascoreOperation.ToLower())
            {
                case "runascorephospho":
                    //We will have to figure out a better way to get the resources (files needed by AScore)
                    //clsAScoreAMGetPhospho ascoreResObj = new clsAScoreAMGetPhospho(m_jobParams, m_mgrParams);
                    //blnSuccess = ascoreResObj.GetAScoreFiles(m_jobParams.GetParam("DataPackageID"));

                    clsAScoreAMRunPhospho ascoreToolObj = new clsAScoreAMRunPhospho(m_jobParams, m_mgrParams);
                    // Attach the progress event handler
                    ascoreToolObj.ProgressChanged += new clsAScoreAMBase.ProgressChangedEventHandler(AScoreProgressChanged);

                    blnSuccess = ascoreToolObj.RunPhospho(m_jobParams.GetParam("DataPackageID"));

                    break;

                default:
                    blnSuccess = false;
                    //m_message = "AScore Operation: " + ascoreOperation + "not recognized.";
                    // Future: throw an error
                    break;
            }
            return blnSuccess;
        }


        void AScoreProgressChanged(object sender, clsAScoreAMBase.ProgressChangedEventArgs e)
        {

            // Update the step tool progress
            // However, AScore routinely reports progress of 0% or 100% at the start and end of certain subtasks, so ignore those values
            //if (e.percentComplete > 0 && e.percentComplete < 100)
            //m_progress = PROGRESS_PCT_ASCORE_START + (PROGRESS_PCT_ASCORE_DONE - PROGRESS_PCT_ASCORE_START) * e.percentComplete / 100.0F;

            //if (!string.IsNullOrEmpty(e.taskDescription))
                //m_CurrentAScoreTask = e.taskDescription;

            //if (System.DateTime.UtcNow.Subtract(m_LastStatusUpdateTime).TotalSeconds >= 10)
            //{
            //    m_LastStatusUpdateTime = System.DateTime.UtcNow;
            //    m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);
            //}
        }

        #endregion


    }
}
