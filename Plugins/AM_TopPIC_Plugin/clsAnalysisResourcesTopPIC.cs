//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerTopPICPlugIn
{
    /// <summary>
    /// Retrieve resources for the TopPIC plugin
    /// </summary>
    public class clsAnalysisResourcesTopPIC : clsAnalysisResources
    {
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
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
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

            if (!FileSearch.RetrieveFile(paramFileName, mJobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve Fasta file
            var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            LogMessage("Getting data files");

            // Find the _ms2.msalign file
            var ms2MSAlignFile = DatasetName + MSALIGN_FILE_SUFFIX;
            var success = FileSearch.FindAndRetrieveMiscFiles(ms2MSAlignFile, false, true, out var sourceDirPath);
            if (!success)
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileToSkip(ms2MSAlignFile);


            // TopPIC 1.2 and earlier created a .feature file
            // TopPIC 1.3 creates two files: _ms1.feature and _ms2.feature

            // Keys in this dictionary are filenames, values are True if the file needs to be unzipped
            var filesToRetrieve = new Dictionary<string, bool>();

            var htmlFileName = DatasetName + "_html.zip";

            var legacyFeatureFile = new FileInfo(Path.Combine(sourceDirPath, DatasetName + TOPFD_FEATURE_FILE_SUFFIX));
            if (legacyFeatureFile.Exists)
            {
                filesToRetrieve.Add(legacyFeatureFile.Name, false);
            }
            else
            {
                filesToRetrieve.Add(DatasetName + "_ms1" + TOPFD_FEATURE_FILE_SUFFIX, false);
                filesToRetrieve.Add(DatasetName + "_ms2" + TOPFD_FEATURE_FILE_SUFFIX, false);

                // Also retrieve the _html.zip file, though it is not required to exist
                // In particular, if the TopFD step for this job used TopFD results from a prior job, the transfer directory will not have an _html.zip file
                filesToRetrieve.Add(htmlFileName, true);
            }

            foreach (var fileToRetrieve in filesToRetrieve)
            {
                var fileName = fileToRetrieve.Key;
                var unzip = fileToRetrieve.Value;

                var fileIsRequired = !fileName.Equals(htmlFileName);

                if (!FileSearch.FindAndRetrieveMiscFiles(fileName, false, true, fileIsRequired))
                {
                    if (fileIsRequired)
                    {
                        // This was a required file; abort
                        // Errors were reported in function call, so just return
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
                    mJobParams.AddResultFileToSkip(fileName);
                    continue;
                }

                // Unzip the file
                // Do not call AddResultFileToSkip on the .zip file since TopPIC will create new files and the zip file will be re-generated

                var zipOutputDirectoryPath = Path.Combine(mWorkDir, DatasetName + "_html");
                LogMessage("Unzipping file " + fileName);
                if (UnzipFileStart(Path.Combine(mWorkDir, fileName), zipOutputDirectoryPath, "clsAnalysisResourcesTopPIC.GetResources"))
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
