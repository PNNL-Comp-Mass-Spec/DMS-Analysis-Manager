' Last modified 06/15/2009 JDS - Added logging using log4net
Imports AnalysisManagerBase
Imports System.IO
Imports System
Imports ParamFileGenerator.MakeParams

Public Class clsAnalysisResourcesIN
    Inherits clsAnalysisResources

    ' As of version 20090202, Inspect supports directly reading _dta.txt files
    ' However, it's possible the Pvalues are not being computed correctly for 3+ spectra
    Public Const DECONCATENATE_DTA_TXT_FILE As Boolean = False


    '*********************************************************************************************************
    'Subclass for Inspect-specific tasks:
    '	1) Distributes OrgDB files 
    '	2) Uses ParamFileGenerator to create param file from database instead of copying it
    '	3) Retrieves zipped DTA files, unzips, and un-concatenates them
    '*********************************************************************************************************

#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Inspect analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve unzipped dta files
        If Not RetrieveDtaFiles(DECONCATENATE_DTA_TXT_FILE) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files

        Dim ext As String
        Dim DumFiles() As String

        'update list of files to be deleted after run
        For Each ext In clsGlobal.m_FilesToDeleteExt
            DumFiles = Directory.GetFiles(m_mgrParams.GetParam("workdir"), "*" & ext) 'Zipped DTA
            For Each FileToDel As String In DumFiles
                clsGlobal.FilesToDelete.Add(FileToDel)
            Next
        Next

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
    ''' </summary>
    ''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Public Overrides Function RetrieveDtaFiles(ByVal UnConcatenate As Boolean) As Boolean

        'Retrieve zipped DTA file
        Dim DtaResultFileName As String
        Dim strUnzippedFileNameRoot As String
        Dim strPathToDelete As String = String.Empty

        Dim CloneStepRenum As String
        Dim stepNum As String
        Dim parallelZipNum As Integer
        Dim isParallelized As Boolean = False

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        CloneStepRenum = m_jobParams.GetParam("CloneStepRenumberStart")
        stepNum = m_jobParams.GetParam("Step")

        'Determine if this is parallelized inspect job
        If System.String.IsNullOrEmpty(CloneStepRenum) Then
            DtaResultFileName = m_jobParams.GetParam("DatasetNum") & "_dta.zip"
            strUnzippedFileNameRoot = m_jobParams.GetParam("DatasetNum")
        Else
            parallelZipNum = CInt(stepNum) - CInt(CloneStepRenum) + 1
            DtaResultFileName = m_jobParams.GetParam("DatasetNum") & "_" & CStr(parallelZipNum) & "_dta.txt"
            strUnzippedFileNameRoot = m_jobParams.GetParam("DatasetNum") & "_" & CStr(parallelZipNum)
            isParallelized = True
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Processing parallelized Inspect segment " & parallelZipNum.ToString)
        End If

        Dim DtaResultFolderName As String = FindDataFile(DtaResultFileName)

        If DtaResultFolderName = "" Then
            ' No folder found containing the zipped DTA files (error will have already been logged)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FindDataFile returned False for " & DtaResultFileName)
            End If
            Return False
        End If

        'Copy the file
        If Not CopyFileToWorkDir(DtaResultFileName, DtaResultFolderName, WorkingDir) Then
            ' Error copying file (error will have already been logged)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & DtaResultFileName & " using folder " & DtaResultFolderName)
            End If
            Return False
        End If

        ' Check to see if the job is parallelized
        '  If it is parallelized, we do not need to unzip the concatenated DTA file (since it is already unzipped)
        '  If not parallelized, then we do need to unzip
        If Not isParallelized OrElse System.IO.Path.GetExtension(DtaResultFileName).ToLower = ".zip" Then
            'Unzip concatenated DTA file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
            If UnzipFileStart(Path.Combine(WorkingDir, DtaResultFileName), WorkingDir, "clsAnalysisResources.RetrieveDtaFiles", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
                End If
            End If
        End If

        'Unconcatenate DTA file if needed
        If UnConcatenate Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated DTA file")
            Dim BackWorker As New System.ComponentModel.BackgroundWorker
            Dim FileSplitter As New clsSplitCattedFiles(BackWorker)
            '				FileSplitter.SplitCattedDTAsOnly(m_jobParams.GetParam("DatasetNum"), WorkingDir)
            FileSplitter.SplitCattedDTAsOnly(strUnzippedFileNameRoot, WorkingDir)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Completed splitting concatenated DTA file")
            End If

            Try
                ' Now that the _dta.txt has been deconcatenated, we need to delete it; if we don't, Inspect will search it too

                strPathToDelete = System.IO.Path.Combine(WorkingDir, strUnzippedFileNameRoot & "_dta.txt")

                If m_DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting concatenated DTA file: " & strPathToDelete)
                End If

                System.Threading.Thread.Sleep(1000)
                System.IO.File.Delete(strPathToDelete)

                If System.IO.File.Exists(strPathToDelete) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Deletion of concatenated DTA file failed: " & strPathToDelete)
                End If

            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception trying to delete file " & strPathToDelete & "; " & ex.Message)
            End Try
        End If

        Return True

    End Function
#End Region

End Class
