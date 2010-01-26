' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerICR
	Inherits clsAnalysisToolRunnerICRBase

    'Performs PEK analysis using ICR-2LS on Bruker S-folder MS data

    ' Example folder layout when processing S-folders 
    '
    ' C:\DMS_WorkDir1\   contains the .Par file
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\   is empty
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001   contains 100 files (see below)
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002   contains another 100 files (see below)
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s003
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s004
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s005
    ' etc.
    ' 
    ' Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001\
    ' 110409_His.00001
    ' 110409_His.00002
    ' 110409_His.00003
    ' ...
    ' 110409_His.00099
    ' 110409_His.00100
    ' 
    ' Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002\
    ' 110409_His.00101
    ' 110409_His.00102
    ' 110409_His.00103
    ' etc.
    ' 
    ' 
	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim ResCode As IJobParams.CloseOutType
        Dim DatasetName As String
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

		'Assemble the dataset name
        DatasetName = m_jobParams.GetParam("datasetNum")
        DSNamePath = CheckTerminator(System.IO.Path.Combine(m_WorkDir, DatasetName))

        'Assemble the output file name and path
        OutFileNamePath = System.IO.Path.Combine(m_WorkDir, DatasetName & ".pek")

        Dim NewSourceFolder As String = AnalysisManagerBase.clsAnalysisResources.ResolveSerStoragePath(m_WorkDir)
        'Check for "0.ser" folder
        If Not String.IsNullOrEmpty(NewSourceFolder) Then
            blnSuccess = MyBase.StartICR2LS(NewSourceFolder, ParamFilePath, OutFileNamePath, ICR2LSProcessingModeConstants.SerFolderPEK, UseAllScans, MinScan, MaxScan)
        Else
            If Not System.IO.Directory.Exists(DSNamePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Data file folder not found: " & DSNamePath)

                CleanupFailedJob("Unable to find data files in working directory")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
            blnSuccess = MyBase.StartICR2LS(DSNamePath, ParamFilePath, OutFileNamePath, ICR2LSProcessingModeConstants.SFoldersPEK, UseAllScans, MinScan, MaxScan)
        End If

        If Not blnSuccess Then
            CleanupFailedJob("Error starting ICR-2LS")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            m_message = AppendToComment(m_message, "Error performing post analysis tasks")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the dataset folder containing s-folders from the working directory
		Dim RetryCount As Integer = 0
        Dim ErrMsg As String = String.Empty

		While RetryCount < 3
			Try
                System.Threading.Thread.Sleep(5000)             'Allow extra time for ICR2LS to release file locks
                If System.IO.Directory.Exists(System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum"))) Then
                    System.IO.Directory.Delete(System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum")), True)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            Catch Err As System.IO.IOException
                'If problem is locked file, retry
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting data file, attempt #" & RetryCount.ToString)
                End If
                ErrMsg = Err.Message
                RetryCount += 1
            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting raw data files, job " & m_JobNum & ": " & Err.Message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try
		End While

		'If we got to here, then we've exceeded the max retry limit
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Unable to delete raw data file after multiple tries, job " & m_JobNum & ", Error " & ErrMsg)
        Return IJobParams.CloseOutType.CLOSEOUT_FAILED

	End Function

End Class
