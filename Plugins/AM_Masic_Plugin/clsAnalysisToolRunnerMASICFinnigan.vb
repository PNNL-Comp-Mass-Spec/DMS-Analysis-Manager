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

	Protected Overrides Function RunMASIC() As IJobParams.CloseOutType

		Dim strParameterFileName As String
		Dim strParameterFilePath As String

		Dim strRawFileName As String
		Dim strInputFilePath As String

		strParameterFileName = m_jobParams.GetParam("parmFileName")

		If Not strParameterFileName Is Nothing AndAlso strParameterFileName.Trim.ToLower <> "na" Then
			strParameterFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
		Else
			strParameterFilePath = String.Empty
		End If

		' Determine the path to the .Raw file
		strRawFileName = m_Dataset & ".raw"
		strInputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strRawFileName)

		If strInputFilePath Is Nothing OrElse strInputFilePath.Length = 0 Then
			' Unable to resolve the file path
			m_ErrorMessage = "Could not find " & strRawFileName & " or " & strRawFileName & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX & " in the working folder; unable to run MASIC"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Examine the size of the .Raw file
		Dim fiInputFile As New IO.FileInfo(IO.Path.Combine(m_WorkDir, strRawFileName))
		If Not fiInputFile.Exists Then
			' Unable to resolve the file path
			m_ErrorMessage = "Could not find " & fiInputFile.FullName & "; unable to run MASIC"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Const TWO_GB As Long = 1024L * 1024 * 1024 * 2
		If fiInputFile.Length > TWO_GB Then
			' .Raw file is over 2 GB in size; convert to mzXML and centroid

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, ".Raw file is over 2 GB; converting to a centroided .mzXML file")

			Dim strMzXMLFilePath As String
			strMzXMLFilePath = ConvertRawToMzXML(fiInputFile)

			If String.IsNullOrEmpty(strMzXMLFilePath) Then
				If String.IsNullOrEmpty(m_message) Then m_message = "Empty path returned by ConvertRawToMzXML for " & fiInputFile.FullName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			strInputFilePath = strMzXMLFilePath

			m_EvalMessage = ".Raw file over 2 GB; converted to a centroided .mzXML file"

		End If

		Return MyBase.StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath)

	End Function

	''' <summary>
	''' Converts the .Raw file specified by fiThermoRawFile to a .mzXML file
	''' </summary>
	''' <param name="fiThermoRawFile"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ConvertRawToMzXML(ByVal fiThermoRawFile As IO.FileInfo) As String

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

		Dim strMzXMLFilePath As String = IO.Path.ChangeExtension(fiThermoRawFile.FullName, "mzXML")
		If Not IO.File.Exists(strMzXMLFilePath) Then
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
			FoundFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.raw")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next MyFile
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error finding .raw files to delete, job " & m_JobNum)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

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
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			m_StatusTools.UpdateAndWrite(m_progress)
		End If
	End Sub

#End Region

End Class
