using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMRunWorkflow : clsApeAMBase
    {
        #region Member Variables
   
        /// <summary>
        /// The parameters for the running a workflow
        /// </summary>
        private static bool _shouldExit = false;

        #endregion

            #region Constructors 

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        /// <param name="monitor"></param>
        public clsApeAMRunWorkflow(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {           
        }

        #endregion

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool RunWorkflow(String dataPackageID)
        {
            bool blnSuccess = true;
            blnSuccess = RunWorkflowAll();
            return blnSuccess;
        }

        protected bool RunWorkflowAll()
        {
            bool blnSuccess = true;
			Ape.SqlConversionHandler mHandle = new Ape.SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (success)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Ape successfully ran workflow" + GetJobParam("ApeWorkflowName"));
                        blnSuccess = true;
                    }
                    else
                    {
                        if (!_shouldExit)
                        {
							mErrorMessage = "Error running Ape";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
                            blnSuccess = false;
                        }
                    }
                }

            });

            string apeWorkflow = Path.Combine(mWorkingDir, GetJobParam("ApeWorkflowName"));
            string apeDatabase = Path.Combine(mWorkingDir, "Results.db3");
            string apeWorkflowStepList = Convert.ToString(GetJobParam("ApeWorkflowStepList"));

			if (string.IsNullOrEmpty(apeWorkflowStepList))
			{
				// The job parameter originally was missing the "k" in workflow; try that version instead
				apeWorkflowStepList = Convert.ToString(GetJobParam("ApeWorflowStepList"));
			}

            //New code
            bool apeCompactDatabase = Convert.ToBoolean(GetJobParam("ApeCompactDatabase"));

			Ape.SqlServerToSQLite.ProgressChanged += new Ape.SqlServerToSQLite.ProgressChangedEventHandler(OnProgressChanged);
			Ape.SqlServerToSQLite.StartWorkflow(apeWorkflowStepList, apeWorkflow, apeDatabase, apeDatabase, false, apeCompactDatabase, mHandle);

            return blnSuccess;
                    
        }
    }
}
