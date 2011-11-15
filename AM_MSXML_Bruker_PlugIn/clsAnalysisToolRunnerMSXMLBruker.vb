'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/30/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerMSXMLBruker
    Inherits clsAnalysisToolRunnerBase

#Region "Module Variables"
    Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 5

    Protected Const COMPASS_XPORT As String = "CompassXport.exe"

    Protected WithEvents mCompassXportRunner As clsCompassXportRunner

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
        Dim eResult As IJobParams.CloseOutType
        Dim eReturnCode As IJobParams.CloseOutType

        ' Set this to success for now
        eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the CompassXport version info in the database
		If Not StoreToolVersionInfo() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			m_message = "Error determining CompassXport version"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        eResult = CreateMSXmlFile()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Something went wrong
            ' In order to help diagnose things, we will move whatever files were created into the eResult folder, 
            '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Error running CompassXport"
            End If

            If eResult = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
                eReturnCode = eResult
            Else
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow

        'Delete the raw data files
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Deleting raw data file")
        End If

        If DeleteRawDataFiles() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Problem deleting raw data files: " & m_message)
            m_message = "Error deleting raw data files"
            ' Don't treat this as a critical error; leave eReturnCode unchanged
        End If

        'Update the job summary file
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Updating summary file")
        End If
        UpdateSummaryFile()

        'Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Making results folder")
        End If

        eResult = MakeResultsFolder()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eResult = MoveResultFiles()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MoveResultFiles moves the eResult files to the eResult folder
            m_message = "Error moving files into results folder"
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eResult = CopyResultsFolderToServer()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return eResult
        End If

        'If we get to here, everything worked so exit happily
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Generate the mzXML or mzML file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function CreateMSXmlFile() As IJobParams.CloseOutType

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLGen.CreateMSXmlFile(): Enter")
        End If

        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' Typically CompassXport.exe

        Dim msXmlFormat As String = m_jobParams.GetParam("MSXMLOutputType")             ' Typically mzXML or mzML
        Dim CentroidMSXML As Boolean = CBool(m_jobParams.GetParam("CentroidMSXML"))

        Dim CompassXportProgramPath As String
        Dim eOutputType As clsCompassXportRunner.MSXMLOutputTypeConstants

        Dim blnSuccess As Boolean

        If msXmlGenerator.ToLower = COMPASS_XPORT.ToLower() Then
            CompassXportProgramPath = m_mgrParams.GetParam("CompassXportLoc")

            If String.IsNullOrEmpty(CompassXportProgramPath) Then
                m_message = "Manager parameter CompassXportLoc is not defined in the Manager Control DB"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If Not System.IO.File.Exists(CompassXportProgramPath) Then
                m_message = "CompassXport program not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & CompassXportProgramPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Else
            m_message = "Invalid value for MSXMLGenerator: " & msXmlGenerator
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eOutputType = clsCompassXportRunner.GetMsXmlOutputTypeByName(msXmlFormat)
        If eOutputType = clsCompassXportRunner.MSXMLOutputTypeConstants.Invalid Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "msXmlFormat string is not mzXML or mzML (" & msXmlFormat & "); will default to mzXML")
            eOutputType = clsCompassXportRunner.MSXMLOutputTypeConstants.mzXML
        End If

        ' Instantiate the processing class
        mCompassXportRunner = New clsCompassXportRunner(m_WorkDir, CompassXportProgramPath, m_Dataset, eOutputType, CentroidMSXML)

        ' Create the file
        blnSuccess = mCompassXportRunner.CreateMSXMLFile

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mCompassXportRunner.ErrorMessage)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ElseIf mCompassXportRunner.ErrorMessage.Length > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mCompassXportRunner.ErrorMessage)

        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)

        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' Typically CompassXport.exe
        If msXmlGenerator.ToLower = COMPASS_XPORT.ToLower() Then
            ioToolFiles.Add(New System.IO.FileInfo(m_mgrParams.GetParam("CompassXportLoc")))
        Else
			m_message = "Invalid value for MSXMLGenerator; should be " & COMPASS_XPORT
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
    ''' Event handler for CompassXportRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CompassXportRunner_LoopWaiting() Handles mCompassXportRunner.LoopWaiting
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
    ''' Event handler for mCompassXportRunner.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub mCompassXportRunner_ProgRunnerStarting(ByVal CommandLine As String) Handles mCompassXportRunner.ProgRunnerStarting
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
    End Sub
#End Region

End Class
