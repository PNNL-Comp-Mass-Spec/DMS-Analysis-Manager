
Option Strict On

Imports System.IO
Imports AnalysisManagerBase
Imports MyEMSLReader

Public Class clsAnalysisResourcesIN
    Inherits clsAnalysisResources

    '*********************************************************************************************************
    'Subclass for Inspect-specific tasks:
    '	1) Distributes OrgDB files 
    '	2) Uses ParamFileGenerator to create param file from database instead of copying it
    '	3) Retrieves zipped DTA files, unzips, and un-concatenates them
    '*********************************************************************************************************

#Region "Methods"

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEMSLUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    ''' <summary>
    ''' Retrieves files necessary for performance of Inspect analysis
    ''' </summary>
    ''' <returns>CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return CloseOutType.CLOSEOUT_FAILED

        'Retrieve param file
        If Not RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the _DTA.txt file
        If Not RetrieveDtaFiles() Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
        m_jobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

        'All finished
        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Public Shadows Function RetrieveDtaFiles() As Boolean

        'Retrieve zipped DTA file
        Dim DtaResultFileName As String

        Dim CloneStepRenum As String
        Dim stepNum As String
        Dim parallelZipNum As Integer
        Dim isParallelized As Boolean = False

        CloneStepRenum = m_jobParams.GetParam("CloneStepRenumberStart")
        stepNum = m_jobParams.GetParam("Step")

        'Determine if this is parallelized inspect job
        If String.IsNullOrEmpty(CloneStepRenum) Then
            DtaResultFileName = DatasetName & "_dta.zip"
        Else
            parallelZipNum = CInt(stepNum) - CInt(CloneStepRenum) + 1
            DtaResultFileName = DatasetName & "_" & CStr(parallelZipNum) & "_dta.txt"
            isParallelized = True
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Processing parallelized Inspect segment " & parallelZipNum.ToString)
        End If

        Dim DtaResultFolderName As String = FindDataFile(DtaResultFileName)

        If String.IsNullOrEmpty(DtaResultFolderName) Then
            ' No folder found containing the zipped DTA files (error will have already been logged)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FindDataFile returned False for " & DtaResultFileName)
            End If
            Return False
        End If

        If DtaResultFolderName.StartsWith(MYEMSL_PATH_FLAG) Then
            If m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Downloaded " + m_MyEMSLUtilities.DownloadedFiles.First().Value.Filename + " from MyEMSL")
                End If
            Else
                Return False
            End If
        Else
            'Copy the file
            If Not CopyFileToWorkDir(DtaResultFileName, DtaResultFolderName, m_WorkingDir) Then
                ' Error copying file (error will have already been logged)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & DtaResultFileName & " using folder " & DtaResultFolderName)
                End If
                Return False
            End If
        End If

        ' Check to see if the job is parallelized
        '  If it is parallelized, we do not need to unzip the concatenated DTA file (since it is already unzipped)
        '  If not parallelized, then we do need to unzip
        If Not isParallelized OrElse Path.GetExtension(DtaResultFileName).ToLower = ".zip" Then
            'Unzip concatenated DTA file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
            If UnzipFileStart(Path.Combine(m_WorkingDir, DtaResultFileName), m_WorkingDir, "clsAnalysisResourcesIN.RetrieveDtaFiles", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
                End If
            End If
        End If

        Return True

    End Function
#End Region

End Class
