Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/15/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerMASICFinnigan
	Inherits clsAnalysisToolRunnerMASICBase

	'*********************************************************************************************************
	'Derived class for performing MASIC analysis on Finnigan datasets
	'*********************************************************************************************************

#Region "Module Variables"
	Protected WithEvents mMSXmlCreator As AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator
#End Region

	Public Sub New()
	End Sub

	Public Shared Function NeedToConvertRawToMzXML(ByVal fiInputFile As FileInfo) As Boolean
		Const TWO_GB As Long = 1024L * 1024 * 1024 * 2

		If fiInputFile.Length > TWO_GB Then Return True

		Return False

	End Function

	Protected Overrides Function RunMASIC() As IJobParams.CloseOutType

		Dim strParameterFilePath As String

		Dim strParameterFileName = m_jobParams.GetParam("parmFileName")

		If Not strParameterFileName Is Nothing AndAlso strParameterFileName.Trim.ToLower <> "na" Then
			strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
		Else
			strParameterFilePath = String.Empty
		End If

		' Determine the path to the .Raw file
		Dim strRawFileName = m_Dataset & ".raw"
		Dim strInputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strRawFileName)

		If String.IsNullOrWhiteSpace(strInputFilePath) Then
			' Unable to resolve the file path
			m_ErrorMessage = "Could not find " & strRawFileName & " or " & strRawFileName & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX & " in the working folder; unable to run MASIC"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Examine the size of the .Raw file
		Dim fiInputFile As New FileInfo(strInputFilePath)
		If Not fiInputFile.Exists Then
			' Unable to resolve the file path
			m_ErrorMessage = "Could not find " & fiInputFile.FullName & "; unable to run MASIC"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not String.IsNullOrEmpty(strParameterFilePath) Then
			' Make sure the parameter file has IncludeHeaders defined and set to True
			ValidateParameterFile(strParameterFilePath)
		End If
		
		Dim strScanStatsFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.SCAN_STATS_FILE_SUFFIX)
		Dim strScanStatsExFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.SCAN_STATS_EX_FILE_SUFFIX)

		Dim fiScanStatsOverrideFile As FileInfo = Nothing
		Dim fiScanStatsExOverrideFile As FileInfo = Nothing

		Dim blnConvertRawToMzXML = NeedToConvertRawToMzXML(fiInputFile)
		Dim eCloseout As IJobParams.CloseOutType

		If blnConvertRawToMzXML Then
			eCloseout = StartConvertRawToMzXML(fiInputFile, strScanStatsFilePath, strScanStatsExFilePath, fiScanStatsOverrideFile, fiScanStatsExOverrideFile, strInputFilePath)
			If eCloseout <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return eCloseout
			End If
		End If

		eCloseout = MyBase.StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath)

		If eCloseout = IJobParams.CloseOutType.CLOSEOUT_SUCCESS AndAlso blnConvertRawToMzXML Then
			eCloseout = ReplaceScanStatsFiles(strScanStatsFilePath, strScanStatsExFilePath, fiScanStatsOverrideFile, fiScanStatsExOverrideFile)
		End If

		Return eCloseout

	End Function

	Private Function ReplaceScanStatsFiles(
	  ByVal strScanStatsFilePath As String,
	  ByVal strScanStatsExFilePath As String,
	  ByVal fiScanStatsOverrideFile As FileInfo,
	  ByVal fiScanStatsExOverrideFile As FileInfo) As IJobParams.CloseOutType

		Try
			' Replace the _ScanStats.txt file created by MASIC with the ScanStats file created in clsAnalysisResourcesMASIC
			If File.Exists(strScanStatsFilePath) Then
				Threading.Thread.Sleep(250)
				PRISM.Processes.clsProgRunner.GarbageCollectNow()

				File.Delete(strScanStatsFilePath)
				Threading.Thread.Sleep(250)
			End If

			' Rename the override file to have the correct name
			fiScanStatsOverrideFile.MoveTo(strScanStatsFilePath)

			If fiScanStatsExOverrideFile.Exists Then
				' Replace the _ScanStatsEx.txt file created by MASIC with the ScanStatsEx file created in clsAnalysisResourcesMASIC
				If File.Exists(strScanStatsExFilePath) Then
					File.Delete(strScanStatsExFilePath)
					Threading.Thread.Sleep(250)
				End If

				' Rename the override file to have the correct name
				fiScanStatsExOverrideFile.MoveTo(strScanStatsExFilePath)
			End If

		Catch ex As Exception
			m_message = "Error replacing the ScanStats files created from the mzXML file with the ScanStats files created from the .Raw file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " (ReplaceScanStatsFiles): " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function


	''' <summary>
	''' Converts the .Raw file specified by fiThermoRawFile to a .mzXML file
	''' </summary>
	''' <param name="fiThermoRawFile"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ConvertRawToMzXML(ByVal fiThermoRawFile As FileInfo) As String

		Dim strMSXmlGeneratorAppPath As String
		Dim blnSuccess As Boolean

		strMSXmlGeneratorAppPath = MyBase.GetMSXmlGeneratorAppPath()

		mMSXmlCreator = New AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(strMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams)

		blnSuccess = mMSXmlCreator.CreateMZXMLFile()

		If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
			m_message = mMSXmlCreator.ErrorMessage
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Unknown error creating the mzXML file for dataset " & m_Dataset
			ElseIf Not m_message.Contains(m_Dataset) Then
				m_message &= "; dataset " & m_Dataset
			End If
		End If

		If Not blnSuccess Then Return String.Empty

		Dim strMzXMLFilePath As String = Path.ChangeExtension(fiThermoRawFile.FullName, "mzXML")
		If Not File.Exists(strMzXMLFilePath) Then
			m_message = "MSXmlCreator did not create the .mzXML file"
			Return String.Empty
		End If

		Return strMzXMLFilePath

	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .raw file from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		'Delete the .raw file
		Try
			FoundFiles = Directory.GetFiles(m_WorkDir, "*.raw")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next MyFile
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error finding .raw files to delete, job " & m_JobNum)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Function StartConvertRawToMzXML(
	 ByVal fiInputFile As FileInfo,
	 ByVal strScanStatsFilePath As String,
	 ByVal strScanStatsExFilePath As String,
	 ByRef fiScanStatsOverrideFile As FileInfo,
	 ByRef fiScanStatsExOverrideFile As FileInfo,
	 ByRef strInputFilePath As String) As IJobParams.CloseOutType

		' .Raw file is over 2 GB in size
		' Will convert it to mzXML and centroid (so that MASIC will use less memory)

		Try

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, ".Raw file is over 2 GB; converting to a centroided .mzXML file")

			' The ScanStats file should have been created by clsAnalysisResourcesMASIC
			' Rename it now so that we can replace the one created by MASIC with the one created by clsAnalysisResourcesMASIC
			fiScanStatsOverrideFile = New FileInfo(strScanStatsFilePath)
			fiScanStatsExOverrideFile = New FileInfo(strScanStatsExFilePath)

			If Not fiScanStatsOverrideFile.Exists Then
				m_message = "ScanStats file not found (should have been created by clsAnalysisResourcesMASIC)"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiScanStatsOverrideFile.FullName)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				Dim strScanStatsOverrideFilePath As String = strScanStatsFilePath & ".override"
				fiScanStatsOverrideFile.MoveTo(strScanStatsOverrideFilePath)
			End If

			If fiScanStatsExOverrideFile.Exists Then
				Dim strScanStatsExOverrideFilePath As String = strScanStatsExFilePath & ".override"
				fiScanStatsExOverrideFile.MoveTo(strScanStatsExOverrideFilePath)
			End If

			Dim strMzXMLFilePath As String
			strMzXMLFilePath = ConvertRawToMzXML(fiInputFile)

			If String.IsNullOrEmpty(strMzXMLFilePath) Then
				If String.IsNullOrEmpty(m_message) Then m_message = "Empty path returned by ConvertRawToMzXML for " & fiInputFile.FullName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			strInputFilePath = strMzXMLFilePath

			m_EvalMessage = ".Raw file over 2 GB; converted to a centroided .mzXML file"

		Catch ex As Exception
			m_message = "Error preparing to convert the Raw file to a MzXML file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " (StartConvertRawToMzXML): " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#Region "Event Handlers"
	Private Sub mMSXmlCreator_DebugEvent(Message As String) Handles mMSXmlCreator.DebugEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Message)
	End Sub

	Private Sub mMSXmlCreator_ErrorEvent(Message As String) Handles mMSXmlCreator.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Message)
	End Sub

	Private Sub mMSXmlCreator_WarningEvent(Message As String) Handles mMSXmlCreator.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Message)
	End Sub

	Private Sub mMSXmlCreator_LoopWaiting() Handles mMSXmlCreator.LoopWaiting
		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = DateTime.UtcNow
			m_StatusTools.UpdateAndWrite(m_progress)
		End If
	End Sub

#End Region

End Class
