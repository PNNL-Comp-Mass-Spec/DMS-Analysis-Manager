//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerTopPICPlugIn
{
    /// <summary>
    /// Retrieve resources for the TopPIC plugin
    /// </summary>
    public class AnalysisResourcesTopPIC : AnalysisResources
    {
        // Ignore Spelling: html, parm, TopFD, MSAlign

        /// <summary>
        /// .feature file created by TopFD
        /// </summary>
        /// <remarks>Tracks LC/MS features</remarks>
        public const string TOPFD_FEATURE_FILE_SUFFIX = ".feature";

        /// <summary>
        /// _ms2.msalign file created by TopFD
        /// </summary>
        public const string MSALIGN_FILE_SUFFIX = "_ms2.msalign";

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

            // Retrieve param file
            var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);

            if (!FileSearchTool.RetrieveFile(paramFileName, mJobParams.GetParam("ParamFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve the FASTA file
            var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            LogMessage("Retrieving the data files required for TopPIC");

            // Find the _ms2.msalign file
            // However, results for FAIMS datasets could have multiple _ms2.msalign files (one for each CV value)

            var ms2MSAlignFilesForFAIMS = string.Format("{0}_*{1}", DatasetName, MSALIGN_FILE_SUFFIX);

            var faimsFileFound = FileSearchTool.FindAndRetrieveMiscFiles(ms2MSAlignFilesForFAIMS, false, true, out var sourceDirPath, false);

            if (faimsFileFound)
            {
                mJobParams.AddResultFileExtensionToSkip(MSALIGN_FILE_SUFFIX);
            }
            else
            {
                var ms2MSAlignFile = DatasetName + MSALIGN_FILE_SUFFIX;

                var success = FileSearchTool.FindAndRetrieveMiscFiles(ms2MSAlignFile, false, true, out sourceDirPath);

                if (!success)
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddResultFileToSkip(ms2MSAlignFile);
            }

            // TopFD for TopPIC 1.2 and earlier created a .feature file
            // TopFD for TopPIC 1.3 creates two files: _ms1.feature and _ms2.feature
            // TopFD for TopPIC 1.5 creates separate feature files for each CV value

            // Keys in this dictionary are filenames, values are true if the file needs to be unzipped
            var filesToRetrieve = new Dictionary<string, bool>();

            var htmlFileName = DatasetName + "_html.zip";

            var legacyFeatureFile = new FileInfo(Path.Combine(sourceDirPath, DatasetName + TOPFD_FEATURE_FILE_SUFFIX));

            if (legacyFeatureFile.Exists)
            {
                filesToRetrieve.Add(legacyFeatureFile.Name, false);
            }
            else
            {
                if (faimsFileFound)
                {
                    filesToRetrieve.Add(string.Format("{0}_*_ms1{1}", DatasetName, TOPFD_FEATURE_FILE_SUFFIX), false);
                    filesToRetrieve.Add(string.Format("{0}_*_ms2{1}", DatasetName, TOPFD_FEATURE_FILE_SUFFIX), false);
                }
                else
                {
                    filesToRetrieve.Add(DatasetName + "_ms1" + TOPFD_FEATURE_FILE_SUFFIX, false);
                    filesToRetrieve.Add(DatasetName + "_ms2" + TOPFD_FEATURE_FILE_SUFFIX, false);
                }

                // Also retrieve the _html.zip file, though it is not required to exist
                // In particular, if the TopFD step for this job used TopFD results from a prior job, the transfer directory will not have an _html.zip file
                filesToRetrieve.Add(htmlFileName, true);
            }

            foreach (var fileToRetrieve in filesToRetrieve)
            {
                var fileName = fileToRetrieve.Key;
                var unzip = fileToRetrieve.Value;

                var fileIsRequired = !fileName.Equals(htmlFileName);

                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileName, false, true, fileIsRequired))
                {
                    if (fileIsRequired)
                    {
                        // This was a required file; abort
                        // Errors were reported in method call, so just return
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    // Create the html subdirectory
                    var htmlDirectory = new DirectoryInfo(Path.Combine(mWorkDir, DatasetName + "_html"));

                    if (!htmlDirectory.Exists)
                        htmlDirectory.Create();

                    continue;
                }

                if (!unzip)
                {
                    var wildcardIndex = fileName.IndexOf("*", StringComparison.OrdinalIgnoreCase);

                    if (wildcardIndex > 0)
                        mJobParams.AddResultFileExtensionToSkip(fileName.Substring(wildcardIndex + 1));
                    else
                        mJobParams.AddResultFileToSkip(fileName);

                    continue;
                }

                // Unzip the file
                // Do not call AddResultFileToSkip on the .zip file since TopPIC will create new files and the zip file will be re-generated

                var zipOutputDirectoryPath = Path.Combine(mWorkDir, DatasetName + "_html");
                LogMessage("Unzipping file " + fileName);

                if (UnzipFileStart(Path.Combine(mWorkDir, fileName), zipOutputDirectoryPath, "AnalysisResourcesTopPIC.GetResources"))
                {
                    continue;
                }

                // Errors should have already been reported
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
