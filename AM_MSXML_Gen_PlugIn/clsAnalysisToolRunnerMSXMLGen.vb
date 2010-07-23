'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 02/06/2009
'
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files


Public Class clsAnalysisToolRunnerMSXMLGen
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running MS XML generator
    'Currently used to generate MZXML or MZML files
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 5

    Protected WithEvents mMSXmlGenReadW As clsMSXMLGenReadW

#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Presently not used</remarks>
    Public Sub New()

    End Sub

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLGen.Setup()")
        End If
    End Sub

    ''' <summary>
    ''' Runs ReadW tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType

        'Start the job timer
        m_StartTime = System.DateTime.Now

        If CreateMZXMLFile() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.Now

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

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

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

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim DatasetName As String = m_jobParams.GetParam("datasetNum")

        Dim InspectDir As String = m_mgrParams.GetParam("InspectDir")                   ' ReadW.exe is stored in the Inspect folder
        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' Typically ReadW.exe

        Dim msXmlFormat As String = m_jobParams.GetParam("MSXMLOutputType")             ' Typically mzXML or mzML
        Dim CentroidMSXML As Boolean = CBool(m_jobParams.GetParam("CentroidMSXML"))

        Dim ReadWProgramPath As String
        Dim eOutputType As clsMSXMLGenReadW.MSXMLOutputTypeConstants

        Dim blnSuccess As Boolean

        ReadWProgramPath = System.IO.Path.Combine(InspectDir, msXmlGenerator)

        Select Case msXmlFormat.ToLower
            Case "mzxml"
                eOutputType = clsMSXMLGenReadW.MSXMLOutputTypeConstants.mzXML
            Case "mzml"
                eOutputType = clsMSXMLGenReadW.MSXMLOutputTypeConstants.mzML
            Case Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "msXmlFormat string is not mzXML or mzML (" & msXmlFormat & "); will default to mzXML")
                eOutputType = clsMSXMLGenReadW.MSXMLOutputTypeConstants.mzXML
        End Select

        ' Instantiate the processing class
        mMSXmlGenReadW = New clsMSXMLGenReadW(WorkingDir, ReadWProgramPath, DatasetName, eOutputType, CentroidMSXML)

        ' Create the file
        blnSuccess = mMSXmlGenReadW.CreateMSXMLFile

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mMSXmlGenReadW.ErrorMessage)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ElseIf mMSXmlGenReadW.ErrorMessage.Length > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mMSXmlGenReadW.ErrorMessage)

        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

#Region "Event Handlers"
    ''' <summary>
    ''' Event handler for MSXmlGenReadW.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub MSXmlGenReadW_LoopWaiting() Handles mMSXmlGenReadW.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_MSXML_GEN_RUNNING, 0, "", "", "", False)
        End If
    End Sub

    ''' <summary>
    ''' Event handler for mMSXmlGenReadW.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub mMSXmlGenReadW_ProgRunnerStarting(ByVal CommandLine As String) Handles mMSXmlGenReadW.ProgRunnerStarting
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
    End Sub
#End Region

End Class
