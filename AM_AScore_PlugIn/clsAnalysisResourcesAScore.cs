using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_AScore_PlugIn
{
    public class clsAnalysisResourcesAScore : clsAnalysisResources
    {
        //public static string AppFilePath = "";
        protected const string ASCORE_INPUT_FILE = "AScoreBatch.xml";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            //Clear out list of files to delete or keep when packaging the blnSuccesss
            clsGlobal.ResetFilesToDeleteOrKeep();

            bool blnSuccess = true;
            blnSuccess = RunAScoreGetResources();

            if (!blnSuccess) return IJobParams.CloseOutType.CLOSEOUT_FAILED;

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }


        /// <summary>
        /// run the AScore pipeline(s) listed in "AScoreOperations" parameter
        /// </summary>
        protected bool RunAScoreGetResources()
        {
            bool blnSuccess = false;

			string ascoreOperations = m_jobParams.GetParam("AScoreOperations");

			if (string.IsNullOrWhiteSpace(ascoreOperations)) {
                m_message = "AScoreOperations parameter is not defined";
				return false;
			}

			foreach (string ascoreOperation in ascoreOperations.Split(','))
            {
				if (!string.IsNullOrWhiteSpace(ascoreOperation)) {
					blnSuccess = RunAScoreOperation(ascoreOperation.Trim());
					if (!blnSuccess) {
						m_message = "Error running AScore resources operation " + ascoreOperation;
						break;
					}
				}
            }

            return blnSuccess;

        }

        /// <summary>
        /// Run a single AScore operation
        /// </summary>
        /// <param name="ascoreOperation"></param>
        /// <returns></returns>
        private bool RunAScoreOperation(string ascoreOperation)
        {
            bool blnSuccess =  true;

			// Note: case statements must be lowercase
            switch (ascoreOperation.ToLower())
            {
                case "runascorephospho":
                    blnSuccess = GetAScoreFiles();
                    break;
                default:
                    // Future: throw an error
                    break;
            }
            return blnSuccess;
        }


        #region AScore Operations

        private bool GetAScoreFiles()
        {
            bool blnSuccess = true;
            string[] SplitString = null;
            string[] FileNameExt = null;

            //Add list the files to delete to global list
            SplitString = m_jobParams.GetParam("TargetJobFileList").Split(',');
            foreach (string row in SplitString)
            {
                FileNameExt = row.Split(':');
                if (FileNameExt[2] == "nocopy")
                {
                    clsGlobal.m_FilesToDeleteExt.Add(FileNameExt[1]);
                }
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreCIDParamFile param file");

            if (!string.IsNullOrEmpty(m_jobParams.GetParam("AScoreCIDParamFile")))
            {
                if (!RetrieveFile(m_jobParams.GetParam("AScoreCIDParamFile"), m_jobParams.GetParam("transferFolderPath"), m_mgrParams.GetParam("workdir")))
                {
                    return false;
                }
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreETDParamFile param file");

            if (!string.IsNullOrEmpty(m_jobParams.GetParam("AScoreETDParamFile")))
            {
                if (!RetrieveFile(m_jobParams.GetParam("AScoreETDParamFile"), m_jobParams.GetParam("transferFolderPath"), m_mgrParams.GetParam("workdir")))
                {
                    return false;
                }
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreHCDParamFile param file");

            if (!string.IsNullOrEmpty(m_jobParams.GetParam("AScoreHCDParamFile")))
            {
                if (!RetrieveFile(m_jobParams.GetParam("AScoreHCDParamFile"), m_jobParams.GetParam("transferFolderPath"), m_mgrParams.GetParam("workdir")))
                {
                    return false;
                }
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreHCDParamFile param file");

            {
                if (!RetrieveAggregateFiles(SplitString))
                {
                    //Errors were reported in function call, so just return
                    return false;
                }
            }

            return blnSuccess;
        }


        private bool BuildInputFile()
        {
            string[] DatasetFiles = null;
            string DatasetType = null;
            string DatasetName = null;
            string DatasetFileName = null;
            string DatasetID = null;
            string WorkDir = m_mgrParams.GetParam("workdir");
            System.IO.StreamWriter inputFile = new System.IO.StreamWriter(System.IO.Path.Combine(WorkDir, ASCORE_INPUT_FILE));

            try
            {
                inputFile.WriteLine("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
                inputFile.WriteLine("<ascore_batch>");
                inputFile.WriteLine("  <settings>");
                inputFile.WriteLine("    <max_threads>4</max_threads>");
                inputFile.WriteLine("  </settings>");

                //update list of files to be deleted after run
                DatasetFiles = System.IO.Directory.GetFiles(WorkDir, "*_syn*.txt");
                foreach (string Dataset in DatasetFiles)
                {
                    DatasetFileName = System.IO.Path.GetFileName(Dataset);

                    // Function RetrieveAggregateFilesRename in clsAnalysisResources in the main analysis manager program
                    //  will have appended _hcd, _etd, or _cid to the synopsis dta, fht, and syn file for each dataset
                    //  The suffix to use is based on text present in the settings file name for each job
                    // However, if the settings file name did not contain HCD, ETD, or CID, then the dta, fht, and syn files
                    //  will not have had a suffix added; in that case, DatasetType will be ".txt"
                    DatasetType = DatasetFileName.Substring(DatasetFileName.ToLower().IndexOf("_syn") + 4, 4);

                    // If DatasetType is ".txt" then change it to an empty string
                    if (DatasetType.ToLower() == ".txt")
                        DatasetType = string.Empty;

                    DatasetName = DatasetFileName.Substring(0, DatasetFileName.Length - (DatasetFileName.Length - DatasetFileName.ToLower().IndexOf("_syn")));
                    inputFile.WriteLine("  <run>");

                    DatasetID = GetDatasetID(DatasetName);

                    if (string.IsNullOrEmpty(DatasetType) || DatasetType == "_cid")
                    {
                        inputFile.WriteLine("    <param_file>" + System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreCIDParamFile")) + "</param_file>");
                    }
                    else if (DatasetType == "_hcd")
                    {
                        inputFile.WriteLine("    <param_file>" + System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreHCDParamFile")) + "</param_file>");
                    }
                    else if (DatasetType == "_etd")
                    {
                        inputFile.WriteLine("    <param_file>" + System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreETDParamFile")) + "</param_file>");
                    }
                    inputFile.WriteLine("    <output_path>" + WorkDir + "</output_path>");
                    inputFile.WriteLine("    <dta_file>" + System.IO.Path.Combine(WorkDir, DatasetName + "_dta" + DatasetType + ".txt") + "</dta_file>");
                    inputFile.WriteLine("    <fht_file>" + System.IO.Path.Combine(WorkDir, DatasetName + "_fht" + DatasetType + ".txt") + "</fht_file>");
                    inputFile.WriteLine("    <syn_file>" + System.IO.Path.Combine(WorkDir, DatasetName + "_syn" + DatasetType + ".txt") + "</syn_file>");
                    inputFile.WriteLine("    <scan_stats_file>" + System.IO.Path.Combine(WorkDir, DatasetName + "_ScanStatsEx" + ".txt") + "</scan_stats_file>");
                    inputFile.WriteLine("    <dataset_id>" + DatasetID + "</dataset_id>");
                    inputFile.WriteLine("  </run>");
                }

                inputFile.WriteLine("</ascore_batch>");

            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating AScore input file" + ex.Message);

            }
            finally
            {
                inputFile.Close();
            }

            return true;

        }

        protected string GetDatasetID(string DatasetName)
        {
            string Dataset_ID = "";
            string[] Dataset_DatasetID = null;

            foreach (string Item in clsGlobal.m_DatasetInfoList)
            {
                Dataset_DatasetID = Item.Split(':');
                if (Dataset_DatasetID[0] == DatasetName)
                {
                    return Dataset_DatasetID[1];
                }
            }

            return Dataset_ID;

        }

        #endregion



    }
}
