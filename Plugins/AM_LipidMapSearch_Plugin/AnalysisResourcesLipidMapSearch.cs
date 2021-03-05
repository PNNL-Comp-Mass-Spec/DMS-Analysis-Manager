using AnalysisManagerBase;
using MyEMSLReader;
using PRISM.Logging;
using System.IO;
using System.Linq;

namespace AnalysisManagerLipidMapSearchPlugIn
{
    /// <summary>
    /// Retrieve resources for the Lipid Map Search plugin
    /// </summary>
    public class AnalysisResourcesLipidMapSearch : AnalysisResources
    {
        /// <summary>
        /// DeconTools peaks file suffix
        /// </summary>
        public const string DECONTOOLS_PEAKS_FILE_SUFFIX = "_peaks.txt";

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
            var strParamFileName = mJobParams.GetParam("ParmFileName");
            var strParamFileStoragePath = mJobParams.GetParam("ParmFileStoragePath");

            if (!FileSearch.RetrieveFile(strParamFileName, strParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            if (!FileSearch.RetrievePNNLOmicsResourceFiles("LipidToolsProgLoc"))
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
            mJobParams.AddResultFileExtensionToSkip(DECONTOOLS_PEAKS_FILE_SUFFIX);
            mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);

            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset XG_lipid_pt5a using Special_Processing of
            //   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml"},
            //   Job2:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml" AND
            //   Dataset LIKE "$Replace($ThisDataset,_Pos,)%NEG"}'
            //
            // Gives these parameters:

            // SourceJob                     = 852150
            // InputFolderName               = "DLS201206180954_Auto852150"
            // DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
            // DatasetArchivePath            = \\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

            // SourceJob2                    = 852151
            // SourceJob2Dataset             = "XG_lipid_pt5aNeg"
            // SourceJob2FolderPath          = "\\proto-3\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"
            // SourceJob2FolderPathArchive   = "\\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"

            var strDeconToolsFolderName = mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "InputFolderName");

            if (string.IsNullOrEmpty(strDeconToolsFolderName))
            {
                mMessage = "InputFolderName step parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (!strDeconToolsFolderName.StartsWith("DLS", System.StringComparison.OrdinalIgnoreCase))
            {
                mMessage = "InputFolderName step parameter is not a DeconTools folder; it should start with DLS and is auto-determined by the SourceJob SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            var strDatasetFolder = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
            var strDatasetFolderArchive = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");

            if (string.IsNullOrEmpty(strDatasetFolder))
            {
                mMessage = "DatasetStoragePath job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (string.IsNullOrEmpty(strDatasetFolderArchive))
            {
                mMessage = "DatasetArchivePath job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            strDatasetFolder = Path.Combine(strDatasetFolder, DatasetName);
            strDatasetFolderArchive = Path.Combine(strDatasetFolderArchive, DatasetName);

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the dataset's .Raw file and DeconTools _peaks.txt file");
            }

            return RetrieveDatasetAndPeaksFile(DatasetName, strDatasetFolder, strDatasetFolderArchive);
        }

        private bool RetrieveSecondDatasetFiles()
        {
            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset XG_lipid_pt5a using Special_Processing of
            //   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml"},
            //   Job2:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml" AND
            //   Dataset LIKE "$Replace($ThisDataset,_Pos,)%NEG"}'
            //
            // Gives these parameters:

            // SourceJob                     = 852150
            // InputFolderName               = "DLS201206180954_Auto852150"
            // DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
            // DatasetArchivePath            = \\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

            // SourceJob2                    = 852151
            // SourceJob2Dataset             = "XG_lipid_pt5aNeg"
            // SourceJob2FolderPath          = "\\proto-3\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"
            // SourceJob2FolderPathArchive   = "\\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"

            var strSourceJob2 = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2");

            if (string.IsNullOrWhiteSpace(strSourceJob2))
            {
                // Second dataset is not defined; that's OK
                return true;
            }

            if (!int.TryParse(strSourceJob2, out var intSourceJob2))
            {
                mMessage = "SourceJob2 is not numeric";
                LogError(mMessage);
                return false;
            }

            if (intSourceJob2 <= 0)
            {
                // Second dataset is not defined; that's OK
                return true;
            }

            var strDataset2 = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2Dataset");
            if (string.IsNullOrEmpty(strDataset2))
            {
                mMessage = "SourceJob2Dataset job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            var strInputFolder = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2FolderPath");
            var strInputFolderArchive = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2FolderPathArchive");

            if (string.IsNullOrEmpty(strInputFolder))
            {
                mMessage = "SourceJob2FolderPath job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (string.IsNullOrEmpty(strInputFolderArchive))
            {
                mMessage = "SourceJob2FolderPathArchive job parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            var diInputFolder = new DirectoryInfo(strInputFolder);
            var diInputFolderArchive = new DirectoryInfo(strInputFolderArchive);

            if (!diInputFolder.Name.StartsWith("DLS", System.StringComparison.OrdinalIgnoreCase))
            {
                mMessage = "SourceJob2FolderPath is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            if (!diInputFolderArchive.Name.StartsWith("DLS", System.StringComparison.OrdinalIgnoreCase))
            {
                mMessage = "SourceJob2FolderPathArchive is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the second dataset's .Raw file and DeconTools _peaks.txt file");
            }

            return RetrieveDatasetAndPeaksFile(strDataset2, diInputFolder.Parent.FullName, diInputFolderArchive.Parent.FullName);
        }

        private bool RetrieveDatasetAndPeaksFile(string strDatasetName, string strDatasetFolderPath, string strDatasetFolderPathArchive)
        {
            string strFileToFind = null;

            // Copy the .Raw file
            // Search the dataset directory first, then the archive folder

            strFileToFind = strDatasetName + DOT_RAW_EXTENSION;
            if (!CopyFileToWorkDir(strFileToFind, strDatasetFolderPath, mWorkDir, BaseLogger.LogLevels.INFO))
            {
                // Raw file not found on the storage server; try the archive
                if (!CopyFileToWorkDir(strFileToFind, strDatasetFolderPathArchive, mWorkDir, BaseLogger.LogLevels.ERROR))
                {
                    // Raw file still not found; try MyEMSL

                    var DSFolderPath = DirectorySearch.FindValidDirectory(strDatasetName, strFileToFind, retrievingInstrumentDataDir: false);
                    if (DSFolderPath.StartsWith(MYEMSL_PATH_FLAG))
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

            mJobParams.AddResultFileExtensionToSkip(DECONTOOLS_PEAKS_FILE_SUFFIX);

            return true;
        }
    }
}
