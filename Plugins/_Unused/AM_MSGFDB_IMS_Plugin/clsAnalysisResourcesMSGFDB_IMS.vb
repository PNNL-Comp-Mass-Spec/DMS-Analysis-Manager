'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports AnalysisManagerBase.AnalysisTool
Imports AnalysisManagerBase.JobConfig
Imports AnalysisManagerBase.StatusReporting
Imports PRISM.Logging

Public Class clsAnalysisResourcesMSGFDB_IMS
    Inherits AnalysisResources

    Public Overrides Sub Setup(stepToolName as String, mgrParams As IMgrParams, jobParams As IJobParams, statusTools as IStatusFile, myEMSLUtilities as MyEMSLUtilities)
        MyBase.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities)
        SetOption(AnalysisManagerBase.Global.AnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As CloseOutType

        ' Make sure the machine has enough free memory to run MSGFDB_IMS
        If Not ValidateFreeMemorySize("MSGFDBJavaMemorySize") Then
            mMessage = "Not enough free memory to run MSGFDB_IMS"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Dim strRawDataType As String = mJobParams.GetParam("RawDataType")
        Dim eRawDataType = GetRawDataType(strRawDataType)

        If eRawDataType <> AnalysisResources.RawDataTypeConstants.UIMF Then
            mMessage = "Dataset type is not compatible with MSGFDB_IMS: " & strRawDataType
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Retrieve Fasta file
        Dim resultCode As CloseOutType

        If Not RetrieveOrgDB(mMgrParams.GetParam("orgdbdir"), resultCode) Then
            Return resultCode
        End If

        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Getting param file")

        ' Retrieve param file
        ' This will also obtain the _ModDefs.txt file using query
        '  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
        '  FROM V_Param_File_Mass_Mod_Info
        '  WHERE Param_File_Name = 'ParamFileName'
        If Not RetrieveGeneratedParamFile(mJobParams.GetParam("ParmFileName")) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Get the UIMF file for this dataset
        If Not FileSearchTool.RetrieveSpectra(strRawDataType) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveDeconToolsResults() Then
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        mJobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION)
        mJobParams.AddResultFileExtensionToSkip("_isos.csv")
        mJobParams.AddResultFileExtensionToSkip("_scans.csv")
        mJobParams.AddResultFileExtensionToSkip("_peaks.txt")
        mJobParams.AddResultFileExtensionToSkip("_peaks.zip")

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RetrieveDeconToolsResults() As Boolean

        ' The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
        ' For example, for dataset BSA_10ugml_IMS6_TOF03_CID_13Aug12_Frodo using Special_Processing of
        '   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "IMS_UIMF_PeakBR2_PeptideBR4_SN3_SumScans4_SumFrames3_noFit_Thrash_WithPeaks_2012-05-09.xml"}
        ' Gives these parameters:

        ' SourceJob                     = 852150
        ' InputFolderName               = "DLS201206180954_Auto852150"
        ' DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
        ' DatasetArchivePath            = \\adms.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

        Dim strDeconToolsFolderName As String
        strDeconToolsFolderName = mJobParams.GetParam("StepParameters", "InputFolderName")

        If String.IsNullOrEmpty(strDeconToolsFolderName) Then
            mMessage = "InputFolderName step parameter not found; this is auto-determined by the SourceJob SpecialProcessing text"
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage)
            Return False

        ElseIf Not strDeconToolsFolderName.ToUpper().StartsWith("DLS") Then
            mMessage = "InputFolderName step parameter is not a DeconTools folder; it should start with DLS and is auto-determined by the SourceJob SpecialProcessing text"
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage)
            Return False
        End If

        Dim intSourceJob As Integer
        intSourceJob = mJobParams.GetJobParameter("SourceJob", 0)

        If intSourceJob = 0 Then
            mMessage = "SourceJob parameter not found; this is auto-defined  by the SourceJob SpecialProcessing text"
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage)
            Return False
        End If

        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Getting DeconTools result files for job " & intSourceJob)

        If Not FileSearchTool.FindAndRetrieveMiscFiles(DatasetName & "_isos.csv", Unzip:=False) Then
            Return False
        End If

        If Not FileSearchTool.FindAndRetrieveMiscFiles(DatasetName & "_scans.csv", Unzip:=False) Then
            Return False
        End If

        Dim strPeaksFileName As String = DatasetName & "_peaks.zip"
        Dim strMatchedPath As String

        ' First look for the zipped version of the _peaks.txt file
        strMatchedPath = FileSearchTool.FindDataFile(strPeaksFileName, searchArchivedDatasetDir:=True, logFileNotFound:=False)
        If Not String.IsNullOrEmpty(strMatchedPath) Then
            ' Zipped version found; retrieve it
            If Not FileSearchTool.FindAndRetrieveMiscFiles(strPeaksFileName, Unzip:=True) Then
                Return False
            End If
        Else
            strPeaksFileName = DatasetName & "_peaks.txt"
            If Not FileSearchTool.FindAndRetrieveMiscFiles(strPeaksFileName, Unzip:=False) Then
                Return False
            End If
        End If

        Return True

    End Function


End Class
