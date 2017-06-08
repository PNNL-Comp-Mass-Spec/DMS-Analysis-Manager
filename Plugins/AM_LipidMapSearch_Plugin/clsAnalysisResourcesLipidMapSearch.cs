using System.IO;
using System.Linq;
using AnalysisManagerBase;
using MyEMSLReader;

namespace AnalysisManagerLipidMapSearchPlugIn
{
    public class clsAnalysisResourcesLipidMapSearch : clsAnalysisResources
    {
        public const string DECONTOOLS_PEAKS_FILE_SUFFIX = "_peaks.txt";

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve the parameter file
            var strParamFileName = m_jobParams.GetParam("ParmFileName");
            var strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

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

            if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool RetrieveFirstDatasetFiles()
        {
            m_jobParams.AddResultFileExtensionToSkip(DECONTOOLS_PEAKS_FILE_SUFFIX);
            m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);

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

            string strDeconToolsFolderName = null;
            strDeconToolsFolderName = m_jobParams.GetParam("StepParameters", "InputFolderName");

            if (string.IsNullOrEmpty(strDeconToolsFolderName))
            {
                m_message = "InputFolderName step parameter not found; this is unexpected";
                LogError(m_message);
                return false;
            }
            else if (!strDeconToolsFolderName.ToUpper().StartsWith("DLS"))
            {
                m_message = "InputFolderName step parameter is not a DeconTools folder; it should start with DLS and is auto-determined by the SourceJob SpecialProcessing text";
                LogError(m_message);
                return false;
            }

            string strDatasetFolder = null;
            string strDatasetFolderArchive = null;

            strDatasetFolder = m_jobParams.GetParam("JobParameters", "DatasetStoragePath");
            strDatasetFolderArchive = m_jobParams.GetParam("JobParameters", "DatasetArchivePath");

            if (string.IsNullOrEmpty(strDatasetFolder))
            {
                m_message = "DatasetStoragePath job parameter not found; this is unexpected";
                LogError(m_message);
                return false;
            }
            else if (string.IsNullOrEmpty(strDatasetFolderArchive))
            {
                m_message = "DatasetArchivePath job parameter not found; this is unexpected";
                LogError(m_message);
                return false;
            }

            strDatasetFolder = Path.Combine(strDatasetFolder, DatasetName);
            strDatasetFolderArchive = Path.Combine(strDatasetFolderArchive, DatasetName);

            if (m_DebugLevel >= 2)
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

            var strSourceJob2 = m_jobParams.GetParam("JobParameters", "SourceJob2");
            var intSourceJob2 = 0;

            if (string.IsNullOrWhiteSpace(strSourceJob2))
            {
                // Second dataset is not defined; that's OK
                return true;
            }

            if (!int.TryParse(strSourceJob2, out intSourceJob2))
            {
                m_message = "SourceJob2 is not numeric";
                LogError(m_message);
                return false;
            }

            if (intSourceJob2 <= 0)
            {
                // Second dataset is not defined; that's OK
                return true;
            }

            string strDataset2 = null;
            strDataset2 = m_jobParams.GetParam("JobParameters", "SourceJob2Dataset");
            if (string.IsNullOrEmpty(strDataset2))
            {
                m_message = "SourceJob2Dataset job parameter not found; this is unexpected";
                LogError(m_message);
                return false;
            }

            string strInputFolder = null;
            string strInputFolderArchive = null;

            strInputFolder = m_jobParams.GetParam("JobParameters", "SourceJob2FolderPath");
            strInputFolderArchive = m_jobParams.GetParam("JobParameters", "SourceJob2FolderPathArchive");

            if (string.IsNullOrEmpty(strInputFolder))
            {
                m_message = "SourceJob2FolderPath job parameter not found; this is unexpected";
                LogError(m_message);
                return false;
            }
            else if (string.IsNullOrEmpty(strInputFolderArchive))
            {
                m_message = "SourceJob2FolderPathArchive job parameter not found; this is unexpected";
                LogError(m_message);
                return false;
            }

            var diInputFolder = new DirectoryInfo(strInputFolder);
            var diInputFolderArchive = new DirectoryInfo(strInputFolderArchive);

            if (!diInputFolder.Name.ToUpper().StartsWith("DLS"))
            {
                m_message = "SourceJob2FolderPath is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text";
                LogError(m_message);
                return false;
            }
            else if (!diInputFolderArchive.Name.ToUpper().StartsWith("DLS"))
            {
                m_message = "SourceJob2FolderPathArchive is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text";
                LogError(m_message);
                return false;
            }

            if (m_DebugLevel >= 2)
            {
                LogDebug("Retrieving the second dataset's .Raw file and DeconTools _peaks.txt file");
            }

            return RetrieveDatasetAndPeaksFile(strDataset2, diInputFolder.Parent.FullName, diInputFolderArchive.Parent.FullName);
        }

        private bool RetrieveDatasetAndPeaksFile(string strDatasetName, string strDatasetFolderPath, string strDatasetFolderPathArchive)
        {
            string strFileToFind = null;

            // Copy the .Raw file
            // Search the dataset folder first, then the archive folder

            strFileToFind = strDatasetName + DOT_RAW_EXTENSION;
            if (!CopyFileToWorkDir(strFileToFind, strDatasetFolderPath, m_WorkingDir, clsLogTools.LogLevels.INFO))
            {
                // Raw file not found on the storage server; try the archive
                if (!CopyFileToWorkDir(strFileToFind, strDatasetFolderPathArchive, m_WorkingDir, clsLogTools.LogLevels.ERROR))
                {
                    // Raw file still not found; try MyEMSL

                    var DSFolderPath = FolderSearch.FindValidFolder(strDatasetName, strFileToFind, RetrievingInstrumentDataFolder: false);
                    if (DSFolderPath.StartsWith(MYEMSL_PATH_FLAG))
                    {
                        // Queue this file for download
                        m_MyEMSLUtilities.AddFileToDownloadQueue(m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo);
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

            m_jobParams.AddResultFileExtensionToSkip(DECONTOOLS_PEAKS_FILE_SUFFIX);

            return true;
        }
    }
}
