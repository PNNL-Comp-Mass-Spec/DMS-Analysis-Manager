' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerLTQ_FTPek
	Inherits clsAnalysisToolRunnerICRBase

    'Performs PEK analysis using ICR-2LS on LTQ-FT MS data

	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim ResCode As IJobParams.CloseOutType
        Dim DSNamePath As String

        Dim MinScan As Integer = 0
        Dim MaxScan As Integer = 0
        Dim UseAllScans As Boolean = True

        Dim OutFileNamePath As String
        Dim ParamFilePath As String
        Dim blnSuccess As Boolean

		'Start with base class function to get settings information
		ResCode = MyBase.RunTool()
		If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

        'Verify a parm file has been specified
        ParamFilePath = System.IO.Path.Combine(m_WorkDir, GetJobParameter(m_jobParams, "parmFileName", ""))
        If Not System.IO.File.Exists(ParamFilePath) Then
            'Param file wasn't specified, but is required for ICR-2LS analysis
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ICR-2LS Param file not found: " & ParamFilePath)

            CleanupFailedJob("Parm file not found")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add handling of settings file info here if it becomes necessary in the future

		'Get scan settings from settings file
        MinScan = 0
        MaxScan = 0

        MinScan = GetJobParameter(m_jobParams, "scanstart", 0)
        MaxScan = GetJobParameter(m_jobParams, "ScanStop", 0)

        If (MinScan = 0 AndAlso MaxScan = 0) OrElse _
           MinScan > MaxScan OrElse _
           MaxScan > 500000 Then
            UseAllScans = True
        Else
            UseAllScans = False
        End If

        'Assemble the data file name and path
        DSNamePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & ".raw")
        If Not System.IO.File.Exists(DSNamePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Raw file not found: " & DSNamePath)

            CleanupFailedJob("Unable to find data file in working directory")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Assemble the output file name and path
        OutFileNamePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & ".pek")

        blnSuccess = MyBase.StartICR2LS(DSNamePath, ParamFilePath, OutFileNamePath, ICR2LSProcessingModeConstants.LTQFTPEK, UseAllScans, MinScan, MaxScan)

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS on file " & DSNamePath)

            ' If a .PEK file exists, then call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
            If VerifyPEKFileExists(m_WorkDir, m_Dataset) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, ".Pek file was found, so will save results to the failed results archive folder")

                PerfPostAnalysisTasks(False)

                ' Try to save whatever files were moved into the results folder
                Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

            Else
                CleanupFailedJob("Error running ICR-2LS (.Pek file not found in " & m_WorkDir & ")")
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks(True) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            m_message = AppendToComment(m_message, "Error performing post analysis tasks")
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
			System.Threading.Thread.Sleep(5000)			 'Allow extra time for ICR2LS to release file locks
            FoundFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.raw")
            For Each MyFile In FoundFiles
                ' Add the file to .FilesToDelete just in case the deletion fails
                clsGlobal.FilesToDelete.Add(MyFile)
                DeleteFileWithRetries(MyFile)
            Next
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting .raw file, job " & m_JobNum & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

End Class

