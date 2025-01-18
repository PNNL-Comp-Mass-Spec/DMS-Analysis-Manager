//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/15/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace AnalysisManagerMSPathFinderPlugin
{
    /// <summary>
    /// Retrieve resources for the MSPathFinder plugin
    /// </summary>
    public class AnalysisResourcesMSPathFinder : AnalysisResources
    {
        // Ignore Spelling: Parm

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (!RetrieveFastaAndParamFile())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            result = RetrieveProMexFeaturesFile();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            result = RetrievePBFFile();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Look for existing .tsv result files
            // These typically will not exist, but may exist if a search was interrupted before it finished
            if (!RetrieveExistingSearchResults())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Look for existing .tsv result files
        /// These will only exist if a search was interrupted before it finished
        /// The files will be in a subdirectory below DMS_FailedResults and will need to have been manually copied to the transfer folder for this job
        /// </summary>
        /// <returns>True if success (even if no files were found); false if an error</returns>
        private bool RetrieveExistingSearchResults()
        {
            var fileSuffixes = new List<string>
            {
                "_IcDecoy.tsv",
                "_IcTarget.tsv",
                "_IcTda.tsv"
            };

            try
            {
                var transferDirectoryPath = GetTransferDirectoryPathForJobStep(useInputDirectory: false);

                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    // Transfer folder parameter is empty; abort the search for result files
                    // This error will be properly dealt with elsewhere
                    return false;
                }

                foreach (var suffix in fileSuffixes)
                {
                    var sourceFile = new FileInfo(Path.Combine(transferDirectoryPath, DatasetName + suffix));

                    if (!sourceFile.Exists)
                    {
                        // File not found; move on to the next file
                        continue;
                    }

                    // Copy the file
                    if (!CopyFileToWorkDir(sourceFile.Name, transferDirectoryPath, mWorkDir, BaseLogger.LogLevels.ERROR))
                    {
                        // Error copying; move on to the next file
                        continue;
                    }
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveExistingSearchResults: " + ex.Message;
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        private bool RetrieveFastaAndParamFile()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve the FASTA file
                var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");

                currentTask = "RetrieveOrgDB to " + localOrgDbFolder;

                if (!RetrieveOrgDB(localOrgDbFolder, out _))
                    return false;

                LogMessage("Retrieving the MSPathFinder param file");

                // Retrieve the parameter file
                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var paramFileName = mJobParams.GetParam("ParamFileName");

                currentTask = "RetrieveGeneratedParamFile " + paramFileName;

                var success = RetrieveGeneratedParamFile(paramFileName);
                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveFastaAndParamFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return false;
            }
        }

        private CloseOutType RetrievePBFFile()
        {
            const string PBF_GEN_FOLDER_PREFIX = "PBF_GEN";

            var currentTask = "Initializing";

            try
            {
                // Cache the input folder name
                var inputFolderNameCached = mJobParams.GetJobParameter("InputFolderName", string.Empty);
                var inputFolderNameWasUpdated = false;

                if (!inputFolderNameCached.ToUpper().StartsWith(PBF_GEN_FOLDER_PREFIX))
                {
                    // Update the input folder to be the PBF_Gen input folder for this job (should be the input_folder of the previous job step)
                    var stepNum = mJobParams.GetJobParameter("Step", 100);

                    // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
                    // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                    var dmsConnectionString = mMgrParams.GetParam("BrokerConnectionString");

                    var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                    var sql = "SELECT input_folder " +
                              "FROM V_Job_Steps_Export " +
                              "WHERE job = " + mJob + " AND step < " + stepNum + " AND input_folder LIKE '" + PBF_GEN_FOLDER_PREFIX + "%' " +
                              "ORDER by step DESC";

                    var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                    RegisterEvents(dbTools);

                    var success = Global.GetQueryResultsTopRow(dbTools, sql, out var inputFolderNameFromDB);

                    if (!success)
                    {
                        mMessage = "Error looking up the correct PBF_Gen folder name in V_Job_Steps_Export";
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    var pbfGenFolderName = inputFolderNameFromDB.FirstOrDefault();

                    if (string.IsNullOrWhiteSpace(pbfGenFolderName))
                    {
                        mMessage = "PBF_Gen folder name listed in V_Job_Steps_Export for step " + (stepNum - 1) + " was empty";
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    mJobParams.SetParam("InputFolderName", pbfGenFolderName);
                    inputFolderNameWasUpdated = true;
                }

                // Retrieve the .pbf file from the MSXml cache folder

                currentTask = "RetrievePBFFile";

                var result = GetPBFFile();

                if (inputFolderNameWasUpdated)
                {
                    mJobParams.SetParam("InputFolderName", inputFolderNameCached);
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrievePBFFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RetrieveProMexFeaturesFile()
        {
            try
            {
                var fileToGet = DatasetName + DOT_MS1FT_EXTENSION;

                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_MS1FT_EXTENSION);
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveProMexFeaturesFile: " + ex.Message;
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
