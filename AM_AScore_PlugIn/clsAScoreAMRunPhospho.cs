using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using AnalysisManagerBase;

namespace AnalysisManager_AScore_PlugIn
{
    class clsAScoreAMRunPhospho : clsAScoreAMBase
    {
        #region Member Variables
   
        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        /// <param name="monitor"></param>
        public clsAScoreAMRunPhospho(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {      
        }

        #endregion

        /// <summary>
        /// Setup and run AScore pipeline according to job parameters
        /// </summary>
        public bool RunPhospho(String dataPackageID)
        {
            bool blnSuccess = true;

            clsAScoreMage dvas = new clsAScoreMage(mJobParms, mMgrParms);
            dvas.Run();


            return blnSuccess;
        }

        protected IJobParams.CloseOutType ConcatenateResultFiles(string FilterExtension)
        {
            string[] ConcatenateAScoreFiles = null;
            string FileToConcatenate = null;
            bool bSkipFirstLine = false;

            try
            {
                ConcatenateAScoreFiles = System.IO.Directory.GetFiles(mWorkingDir, "*" + FilterExtension);
                // Create an instance of StreamWriter to write to a file.
                System.IO.StreamWriter inputFile = new System.IO.StreamWriter(System.IO.Path.Combine(mWorkingDir, "Concatenated" + FilterExtension));

                foreach (string FullFileToConcatenate in ConcatenateAScoreFiles)
                {
                    FileToConcatenate = System.IO.Path.GetFileName(FullFileToConcatenate);

                    // Create an instance of StreamReader to read from a file.
                    System.IO.StreamReader inputBase = new System.IO.StreamReader(System.IO.Path.Combine(mWorkingDir, FileToConcatenate));

                    string inpLine = null;
                    if (bSkipFirstLine)
                    {
                        inputBase.ReadLine();
                    }
                    else
                    {
                        // Skip the first line (the header line) on subsequent files
                        bSkipFirstLine = true;
                    }

                    do
                    {
                        inpLine = inputBase.ReadLine();
                        if ((inpLine != null))
                        {
                            inputFile.WriteLine(inpLine);
                        }
                    } while (!(inpLine == null));
                    inputBase.Close();

                }
                inputFile.Close();

            }
            catch (Exception E)
            {
                // Let the user know what went wrong.
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerPhosphoFdrAggregator.ConcatenateResultFiles, The file could not be concatenated: " + FileToConcatenate + E.Message);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }


    }
}
