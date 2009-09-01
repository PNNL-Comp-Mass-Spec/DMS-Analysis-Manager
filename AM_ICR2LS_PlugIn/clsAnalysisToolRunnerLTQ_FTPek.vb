' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerLTQ_FTPek
	Inherits clsAnalysisToolRunnerICRBase

	'Performs PEK analysis using ICR2LS on LTQ-FT MS data

	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim ResCode As IJobParams.CloseOutType
		Dim DSNamePath As String
		Dim PekRes As Boolean
		Dim MinScan As Integer
		Dim MaxScan As Integer
		Dim NumScans As Integer
		Dim UseAllScans As Boolean
		Dim OutFileNamePath As String

		'Start with base class function to get settings information
		ResCode = MyBase.RunTool()
		If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

		'Verify a parm file has been specified
		If Not File.Exists(Path.Combine(m_workdir, m_jobParams.GetParam("parmFileName"))) Then
			'Parm file wasn't specified, but is required for ICR2LS analysis
			CleanupFailedJob("Parm file not found")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Get scan settings from settings file
        MinScan = CInt(m_jobParams.GetParam("scanstart"))
        MaxScan = CInt(m_jobParams.GetParam("ScanStop"))
		NumScans = MaxScan - MinScan
		UseAllScans = CBool(IIf(MaxScan > 500000, True, False))

		'Assemble the data file name and path
		DSNamePath = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".raw")
		If Not File.Exists(DSNamePath) Then
			CleanupFailedJob("Unable to find data file in working directory")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Assemble the output file name and path
		OutFileNamePath = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".pek")

		'Make the PEK file
		m_JobRunning = True
		If UseAllScans Then
			'Process all scans in input file
			PekRes = m_ICR2LSObj.MakeLTQ_FTPEKFile(DSNamePath, Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName")), _
			 OutFileNamePath)
		Else
			'Process range of scans
			PekRes = m_ICR2LSObj.MakeLTQ_FTPEKFile(DSNamePath, Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName")), _
			 OutFileNamePath, False, NumScans, MinScan)
		End If
		If Not PekRes Then
			CleanupFailedJob("Error creating PEK file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Wait for the job to complete
		If Not WaitForJobToFinish() Then
			CleanupFailedJob("Error waiting for PEK job to finish")
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

		'Deletes the .raw file from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		'Delete the .raw file
		Try
			System.Threading.Thread.Sleep(5000)			 'Allow extra time for ICR2LS to release file locks
			FoundFiles = Directory.GetFiles(m_workdir, "*.raw")
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

