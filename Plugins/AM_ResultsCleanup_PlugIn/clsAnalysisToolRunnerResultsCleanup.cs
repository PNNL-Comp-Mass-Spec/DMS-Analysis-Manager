'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 04/17/2013
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerResultsCleanup
	Inherits clsAnalysisToolRunnerBase

#Region "Constants"
	Protected Const RESULTS_DB3_FILE As String = "Results.db3"
#End Region

#Region "Methods"

	''' <summary>
	''' Runs ResultsCleanup tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As CloseOutType

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
				Return CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the AnalysisManager version info in the database
			If Not StoreToolVersionInfo() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining AnalysisManager version"
				Return CloseOutType.CLOSEOUT_FAILED
			End If

			' Cleanup results in the transfer directory
			Dim Result As CloseOutType
			Result = PerformResultsCleanup()
			If Result <> CloseOutType.CLOSEOUT_SUCCESS Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error calling PerformResultsCleanup"
				End If
				Return CloseOutType.CLOSEOUT_FAILED
			End If

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

		Catch ex As Exception
			m_message = "Error in clsAnalysisToolRunnerResultsCleanup->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return CloseOutType.CLOSEOUT_FAILED
		End Try

		'If we got to here, everything worked, so exit
		Return CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function PerformResultsCleanup() As CloseOutType

		Dim strTransferDirectoryPath As String
		Dim strResultsFolderName As String
		Dim eResult As CloseOutType = CloseOutType.CLOSEOUT_SUCCESS

		Try

			strTransferDirectoryPath = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", String.Empty)
			strResultsFolderName = m_jobParams.GetJobParameter("JobParameters", "InputFolderName", String.Empty)

			If String.IsNullOrWhiteSpace(strTransferDirectoryPath) Then
				m_message = "transferFolderPath not defined"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return CloseOutType.CLOSEOUT_FAILED
			ElseIf String.IsNullOrWhiteSpace(strResultsFolderName) Then
				m_message = "InputFolderName not defined"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return CloseOutType.CLOSEOUT_FAILED
			End If

			Dim diTransferFolder As IO.DirectoryInfo = New IO.DirectoryInfo(strTransferDirectoryPath)

			If Not diTransferFolder.Exists Then
				m_message = "transferFolder not found at " & strTransferDirectoryPath
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return CloseOutType.CLOSEOUT_FAILED
			End If

			Dim diResultsFolder As IO.DirectoryInfo
			diResultsFolder = New IO.DirectoryInfo(IO.Path.Combine(diTransferFolder.FullName, strResultsFolderName))
			eResult = RemoveOldResultsDb3Files(diResultsFolder)

		Catch ex As Exception
			m_message = "Error in PerformResultsCleanup"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return CloseOutType.CLOSEOUT_FAILED
		End Try

		Return eResult

	End Function

	Protected Function RemoveOldResultsDb3Files(ByVal diResultsFolder As IO.DirectoryInfo) As CloseOutType

		Dim reStepNumber As Text.RegularExpressions.Regex
		Dim reMatch As Text.RegularExpressions.Match

		Dim intStepFolderCount As Integer = 0
		Dim dctResultsFiles As Generic.Dictionary(Of Integer, IO.FileInfo)
		Dim intStepNumber As Integer

		Try
			reStepNumber = New Text.RegularExpressions.Regex("Step_(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
			dctResultsFiles = New Generic.Dictionary(Of Integer, IO.FileInfo)

			' Look for Results.db3 files in the subfolders of the transfer folder
			' Only process folders that start with the text "Step_"
			For Each diSubfolder In diResultsFolder.GetDirectories("Step_*")
				intStepFolderCount += 1

				' Parse out the step number
				reMatch = reStepNumber.Match(diSubfolder.Name)

				If reMatch.Success AndAlso Integer.TryParse(reMatch.Groups(1).Value, intStepNumber) Then
					If Not dctResultsFiles.ContainsKey(intStepNumber) Then
						For Each fiFile As IO.FileInfo In diSubfolder.GetFiles(RESULTS_DB3_FILE)
							dctResultsFiles.Add(intStepNumber, fiFile)
							Exit For
						Next
					End If
				End If
			Next

			If dctResultsFiles.Count > 1 Then
				' Delete the Results.db3 files for the steps prior to intLastStep
				Dim intLastStep As Integer = dctResultsFiles.Keys.Max
				Dim intFileCountDeleted As Integer = 0

				Dim lnqQuery = From item In dctResultsFiles Where item.Key < intLastStep
				For Each item In lnqQuery
					Try
						item.Value.Delete()
						intFileCountDeleted += 1
					Catch ex As Exception
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error deleting extra " & RESULTS_DB3_FILE & " file: " & ex.Message)
					End Try
				Next

				m_EvalMessage = "Deleted " & intFileCountDeleted & " extra " & RESULTS_DB3_FILE & " " & clsGlobal.CheckPlural(intFileCountDeleted, "file", "files")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_EvalMessage & " from " & diResultsFolder.FullName)

			ElseIf dctResultsFiles.Count = 1 Then
				m_EvalMessage = "Results folder has just one " & RESULTS_DB3_FILE & " file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_EvalMessage)
			Else
				If intStepFolderCount > 0 Then
					m_EvalMessage = "None of the Step_# folders has a " & RESULTS_DB3_FILE & " file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
				Else
					m_message = "Results folder does not have any Step_# folders"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & diResultsFolder.FullName)
					Return CloseOutType.CLOSEOUT_FAILED
				End If
			End If

		Catch ex As Exception
			m_message = "Error in RemoveOldResultsDb3Files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return CloseOutType.CLOSEOUT_FAILED
		End Try

		Return CloseOutType.CLOSEOUT_SUCCESS

	End Function
	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim strAppFolderPath As String = clsGlobal.GetAppFolderPath()

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of the Analysis Manager
		If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "AnalysisManagerProg") Then
			Return False
		End If

		' Lookup the version of AnalysisManagerResultsCleanupPlugin
		If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "AnalysisManagerResultsCleanupPlugin") Then
			Return False
		End If

		' Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsCleanupPlugin.dll in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerProg.exe")))
		ioToolFiles.Add(New FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerResultsCleanupPlugin.dll")))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

#End Region
End Class
