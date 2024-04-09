using MyEMSLReader;
using PRISM.Logging;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerLipidMapSearchPlugIn
{
    /// <summary>
    /// Retrieve resources for the Lipid Map Search plugin
    /// </summary>
    public class AnalysisResourcesLipidMapSearch : AnalysisResources
    {
        // Ignore Spelling: centroided, dmsarch, Lipidomics, Pos

        /// <summary>
        /// DeconTools peaks file suffix
        /// </summary>
        public const string DECON_TOOLS_PEAKS_FILE_SUFFIX = "_peaks.txt";

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

            // Retrieve the parameter file
            var paramFileName = mJobParams.GetParam("ParamFileName");
            var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

            if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            if (!FileSearchTool.RetrievePNNLOmicsResourceFiles("LipidToolsProgLoc"))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the .Raw file and _Peaks.txt file for this dataset
            if (!RetrieveFirstDatasetFiles())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Potentially retrieve the .Raw file and _Peaks.txt file for the second dataset to be used by this job
            if (!RetrieveSecondDatasetFiles())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool RetrieveFirstDatasetFiles()
        {
            mJobParams.AddResultFileExtensionToSkip(DECON_TOOLS_PEAKS_FILE_SUFFIX);
            mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);

            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset XG_lipid_pt5a using Special_Processing of
            //   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Param File] = "LTQ_FT_Lipidomics_2012-04-16.xml"},
            //   Job2:Auto{Tool = "Decon2LS_V2" AND [Param File] = "LTQ_FT_Lipidomics_2012-04-16.xml" AND
            //   Dataset LIKE "$Replace($ThisDataset,_Pos,)%NEG"}'

            // Gives these parameters:

            // SourceJob                     = 852150
            // InputFolderName               = "DLS201206180954_Auto852150"
            // DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
            // DatasetArchivePath            = \\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

            // SourceJob2                    = 852151
            // SourceJob2Dataset             = "XG_lipid_pt5aNeg"
            // SourceJob2FolderPath          = "\\proto-3\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"
            // SourceJob2FolderPathArchive   = "\\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"

            var deconToolsFolderName = mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "InputFolderName");

            if (string.IsNullOrEmpty(deconToolsFolderName))
            {
                mMessage = "InputFolderName step parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (!deconToolsFolderName.StartsWith("DLS", System.StringComparison.OrdinalIgnoreCase))
            {
                mMessage = "InputFolderName step parameter is not a DeconTools folder; it should start with DLS and is auto-determined by the SourceJob SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            var datasetFolder = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
            var datasetFolderArchive = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");

            if (string.IsNullOrEmpty(datasetFolder))
            {
                mMessage = "DatasetStoragePath job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (string.IsNullOrEmpty(datasetFolderArchive))
            {
                mMessage = "DatasetArchivePath job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            datasetFolder = Path.Combine(datasetFolder, DatasetName);
            datasetFolderArchive = Path.Combine(datasetFolderArchive, DatasetName);

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the dataset's .Raw file and DeconTools _peaks.txt file");
            }

            return RetrieveDatasetAndPeaksFile(DatasetName, datasetFolder, datasetFolderArchive);
        }

        private bool RetrieveSecondDatasetFiles()
        {
            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset XG_lipid_pt5a using Special_Processing of
            //   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Param File] = "LTQ_FT_Lipidomics_2012-04-16.xml"},
            //   Job2:Auto{Tool = "Decon2LS_V2" AND [Param File] = "LTQ_FT_Lipidomics_2012-04-16.xml" AND
            //   Dataset LIKE "$Replace($ThisDataset,_Pos,)%NEG"}'

            // Gives these parameters:

            // SourceJob                     = 852150
            // InputFolderName               = "DLS201206180954_Auto852150"
            // DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
            // DatasetArchivePath            = \\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

            // SourceJob2                    = 852151
            // SourceJob2Dataset             = "XG_lipid_pt5aNeg"
            // SourceJob2FolderPath          = "\\proto-3\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"
            // SourceJob2FolderPathArchive   = "\\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"

            var sourceJob2Text = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2");

            if (string.IsNullOrWhiteSpace(sourceJob2Text))
            {
                // Second dataset is not defined; that's OK
                return true;
            }

            if (!int.TryParse(sourceJob2Text, out var sourceJob2))
            {
                mMessage = "SourceJob2 is not numeric";
                LogError(mMessage);
                return false;
            }

            if (sourceJob2 <= 0)
            {
                // Second dataset is not defined; that's OK
                return true;
            }

            var dataset2 = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2Dataset");

            if (string.IsNullOrEmpty(dataset2))
            {
                mMessage = "SourceJob2Dataset job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            var inputFolderPath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2FolderPath");
            var inputFolderPathArchive = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2FolderPathArchive");

            if (string.IsNullOrEmpty(inputFolderPath))
            {
                mMessage = "SourceJob2FolderPath job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (string.IsNullOrEmpty(inputFolderPathArchive))
            {
                mMessage = "SourceJob2FolderPathArchive job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            var inputFolder = new DirectoryInfo(inputFolderPath);
            var inputFolderArchive = new DirectoryInfo(inputFolderPathArchive);

            if (!inputFolder.Name.StartsWith("DLS", System.StringComparison.OrdinalIgnoreCase))
            {
                mMessage = "SourceJob2FolderPath is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            if (!inputFolderArchive.Name.StartsWith("DLS", System.StringComparison.OrdinalIgnoreCase))
            {
                mMessage = "SourceJob2FolderPathArchive is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the second dataset's .Raw file and DeconTools _peaks.txt file");
            }

            return RetrieveDatasetAndPeaksFile(dataset2, inputFolder.Parent.FullName, inputFolderArchive.Parent.FullName);
        }

        private bool RetrieveDatasetAndPeaksFile(string datasetName, string datasetFolderPath, string datasetFolderPathArchive)
        {
            // Copy the .Raw file
            // Search the dataset directory first, then the archive folder

            var fileToFind = datasetName + DOT_RAW_EXTENSION;

            if (!CopyFileToWorkDir(fileToFind, datasetFolderPath, mWorkDir, BaseLogger.LogLevels.INFO))
            {
                // Raw file not found on the storage server; try the archive
                if (!CopyFileToWorkDir(fileToFind, datasetFolderPathArchive, mWorkDir, BaseLogger.LogLevels.ERROR))
                {
                    // Raw file still not found; try MyEMSL

                    var datasetDirectoryPath = DirectorySearchTool.FindValidDirectory(datasetName, fileToFind, retrievingInstrumentDataDir: false);

                    if (datasetDirectoryPath.StartsWith(MYEMSL_PATH_FLAG))
                    {
                        // Queue this file for download
                        mMyEMSLUtilities.AddFileToDownloadQueue(mMyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo);
                    }
                    else
                    {
                        // Raw file still not found; abort processing
                        return false;
                    }
                }
            }

            // As of January 2013, the _peaks.txt file generated by DeconTools does not have accurate data for centroided spectra
            // Therefore, rather than copying the _Peaks.txt file locally, we will allow the LipidTools.exe software to re-generate it

            mJobParams.AddResultFileExtensionToSkip(DECON_TOOLS_PEAKS_FILE_SUFFIX);

            return true;
        }
    }
}
