'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 02/06/2009
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerMSXMLGen
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running MS XML generator
    'Currently used to generate MZXML or MZML files
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 5

    Protected WithEvents mMSXmlGen As clsMSXmlGen

	Protected mMSXmlGeneratorAppPath As String = String.Empty
#End Region

#Region "Methods"

	''' <summary>
	''' Runs ReadW tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType
		Dim result As IJobParams.CloseOutType

		'Do the base class stuff
		If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Store the ReadW or MSConvert version info in the database
		If Not StoreToolVersionInfo() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			m_message = "Error determining MSXMLGen version"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If


		If CreateMZXMLFile() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		'Stop the job timer
		m_StopTime = System.DateTime.UtcNow

		'Add the current job data to the summary file
		If Not UpdateSummaryFile() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
		End If

		result = MakeResultsFolder()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: What do we do here?
			Return result
		End If

		result = MoveResultFiles()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: What do we do here?
			Return result
		End If

		result = CopyResultsFolderToServer()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: What do we do here?
			Return result
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'No failures so everything must have succeeded

	End Function

	''' <summary>
	''' Generate the mzXML or mzML file
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Private Function CreateMZXMLFile() As IJobParams.CloseOutType

		If m_DebugLevel > 4 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLGen.CreateMZXMLFile(): Enter")
		End If

		Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")			' ReadW.exe or MSConvert.exe

		Dim msXmlFormat As String = m_jobParams.GetParam("MSXMLOutputType")				' Typically mzXML or mzML
		Dim CentroidMSXML As Boolean
		Dim CentroidMS1 As Boolean
		Dim CentroidMS2 As Boolean
		Dim CentroidPeakCountToRetain As Integer

		Dim eOutputType As clsMSXmlGen.MSXMLOutputTypeConstants

		Dim blnSuccess As Boolean

		' Determine the output type
		Select Case msXmlFormat.ToLower
			Case "mzxml"
				eOutputType = clsMSXmlGen.MSXMLOutputTypeConstants.mzXML
			Case "mzml"
				eOutputType = clsMSXmlGen.MSXMLOutputTypeConstants.mzML
			Case Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "msXmlFormat string is not mzXML or mzML (" & msXmlFormat & "); will default to mzXML")
				eOutputType = clsMSXmlGen.MSXMLOutputTypeConstants.mzXML
		End Select


		' Lookup Centroid Settings
		CentroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", False)
		If CentroidMSXML Then
			CentroidMS1 = True
			CentroidMS2 = True
		Else
			CentroidMS1 = m_jobParams.GetJobParameter("CentroidMS1", False)
			CentroidMS2 = m_jobParams.GetJobParameter("CentroidMS2", False)
		End If

		' Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
		CentroidPeakCountToRetain = m_JobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0)

		If CentroidPeakCountToRetain = 0 Then
			' Look for parameter CentroidPeakCountToRetain in any section
			CentroidPeakCountToRetain = m_JobParams.GetJobParameter("CentroidPeakCountToRetain", clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)
		End If

		If String.IsNullOrEmpty(mMSXmlGeneratorAppPath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "mMSXmlGeneratorAppPath is empty; this is unexpected")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Determine the program path and Instantiate the processing class
		If msXmlGenerator.ToLower.Contains("readw") Then
			' ReadW
			' mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

			mMSXmlGen = New clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eOutputType, CentroidMS1 Or CentroidMS2)

		ElseIf msXmlGenerator.ToLower.Contains("msconvert") Then
			' MSConvert

			mMSXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eOutputType, CentroidMS1, CentroidMS2, CentroidPeakCountToRetain)

		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unsupported XmlGenerator: " & msXmlGenerator)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		mMSXmlGen.DebugLevel = m_DebugLevel

		If Not IO.File.Exists(mMSXmlGeneratorAppPath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MsXmlGenerator not found: " & mMSXmlGeneratorAppPath)
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Create the file
		blnSuccess = mMSXmlGen.CreateMSXMLFile

		If Not blnSuccess Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSXmlGen.ErrorMessage)
			m_message = mMSXmlGen.ErrorMessage
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		ElseIf mMSXmlGen.ErrorMessage.Length > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSXmlGen.ErrorMessage)
			m_message = mMSXmlGen.ErrorMessage
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function


	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of IO.FileInfo)

		' Determine the path to the XML Generator
		Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")			' ReadW.exe or MSConvert.exe

		mMSXmlGeneratorAppPath = String.Empty
		If msXmlGenerator.ToLower().Contains("readw") Then
			' ReadW
			' Note that msXmlGenerator will likely be ReAdW.exe
			mMSXmlGeneratorAppPath = MyBase.DetermineProgramLocation("ReAdW", "ReAdWProgLoc", msXmlGenerator)

		ElseIf msXmlGenerator.ToLower().Contains("msconvert") Then
			' MSConvert
			Dim ProteoWizardDir As String = m_mgrParams.GetParam("ProteoWizardDir")			' MSConvert.exe is stored in the ProteoWizard folder
			mMSXmlGeneratorAppPath = IO.Path.Combine(ProteoWizardDir, msXmlGenerator)

		Else
			m_message = "Invalid value for MSXMLGenerator; should be 'ReadW' or 'MSConvert'"
			Return False
		End If

		If Not String.IsNullOrEmpty(mMSXmlGeneratorAppPath) Then
			ioToolFiles.Add(New IO.FileInfo(mMSXmlGeneratorAppPath))
		Else
			' Invalid value for ProgramPath
			m_message = "MSXMLGenerator program path is empty"
			Return False
		End If

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

#End Region

#Region "Event Handlers"
    ''' <summary>
    ''' Event handler for MSXmlGenReadW.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub MSXmlGenReadW_LoopWaiting() Handles mMSXmlGen.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_MSXML_GEN_RUNNING, 0, "", "", "", False)
        End If
    End Sub

    ''' <summary>
    ''' Event handler for mMSXmlGen.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub mMSXmlGen_ProgRunnerStarting(ByVal CommandLine As String) Handles mMSXmlGen.ProgRunnerStarting
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
    End Sub
#End Region

End Class
