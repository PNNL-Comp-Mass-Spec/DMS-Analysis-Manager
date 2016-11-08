Option Strict On

Imports AnalysisManagerBase
Imports System.IO

''' <summary>
''' Performs PEK analysis using ICR-2LS on LTQ-FT MS data
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerLTQ_FTPek
    Inherits clsAnalysisToolRunnerICRBase

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim ResCode As IJobParams.CloseOutType
        Dim DSNamePath As String

        Dim UseAllScans As Boolean

        Dim OutFileNamePath As String
        Dim ParamFilePath As String
        Dim blnSuccess As Boolean

        'Start with base class function to get settings information
        ResCode = MyBase.RunTool()
        If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

        ' Store the ICR2LS version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining ICR2LS version"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Verify a param file has been specified
        ParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
        If Not File.Exists(ParamFilePath) Then
            'Param file wasn't specified, but is required for ICR-2LS analysis
            m_message = "ICR-2LS Param file not found"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ParamFilePath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add handling of settings file info here if it becomes necessary in the future

        'Get scan settings from settings file
        Dim MinScan = m_jobParams.GetJobParameter("scanstart", 0)
        Dim MaxScan = m_jobParams.GetJobParameter("ScanStop", 0)

        ' Determine whether or not we should be processing MS2 spectra
        Dim SkipMS2 = Not m_jobParams.GetJobParameter("ProcessMS2", False)

        If (MinScan = 0 AndAlso MaxScan = 0) OrElse
           MinScan > MaxScan OrElse
           MaxScan > 500000 Then
            UseAllScans = True
        Else
            UseAllScans = False
        End If

        'Assemble the data file name and path
        DSNamePath = Path.Combine(m_WorkDir, m_Dataset & ".raw")
        If Not File.Exists(DSNamePath) Then
            m_message = "Raw file not found: " & DSNamePath
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Assemble the output file name and path
        OutFileNamePath = Path.Combine(m_WorkDir, m_Dataset & ".pek")

        blnSuccess = MyBase.StartICR2LS(DSNamePath, ParamFilePath, OutFileNamePath, ICR2LSProcessingModeConstants.LTQFTPEK, UseAllScans, SkipMS2, MinScan, MaxScan)

        If blnSuccess Then
            If Not VerifyPEKFileExists(m_WorkDir, m_Dataset) Then
                m_message = "ICR-2LS successfully finished but did not make a .Pek file; if all spectra are MS/MS use settings file LTQ_FTPEK_ProcessMS2.txt"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Else

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS on file " & DSNamePath)

            ' If a .PEK file exists, then call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
            If VerifyPEKFileExists(m_WorkDir, m_Dataset) Then
                m_message = "ICR-2LS returned false (see .PEK file in Failed results folder)"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, ".Pek file was found, so will save results to the failed results archive folder")

                PerfPostAnalysisTasks(False)

                ' Try to save whatever files were moved into the results folder
                Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

            Else
                m_message = "Error running ICR-2LS (.Pek file not found in " & m_WorkDir & ")"
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks(True) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Error performing post analysis tasks"
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

        'Deletes the .raw file from the working directory
        Dim FoundFiles() As String
        Dim MyFile As String

        'Delete the .raw file
        Try
            Threading.Thread.Sleep(5000)             'Allow extra time for ICR2LS to release file locks
            FoundFiles = Directory.GetFiles(m_WorkDir, "*.raw")
            For Each MyFile In FoundFiles
                ' Add the file to .FilesToDelete just in case the deletion fails
                m_jobParams.AddResultFileToSkip(MyFile)
                DeleteFileWithRetries(MyFile)
            Next
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting .raw file, job " & m_JobNum & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

End Class

