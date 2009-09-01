' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Public MustInherit Class clsAnalysisToolRunnerICRBase
	Inherits clsAnalysisToolRunnerBase

	'ICR2LS object for use in analysis
    Protected m_ICR2LSObj As New clsICR2LSWrapper()

	'Enumerated constants
    Public Enum ICR_STATUS As Short
        'TODO: This list must be kept current with ICR2LS
        STATE_IDLE = 1
        STATE_PROCESSING = 2
        STATE_KILLED = 3
        STATE_FINISHED = 4
        STATE_GENERATING = 5
        STATE_TICGENERATION = 6
        STATE_LCQTICGENERATION = 7
        STATE_QTOFPEKGENERATION = 8
        STATE_MMTOFPEKGENERATION = 9
        STATE_LTQFTPEKGENERATION = 10
    End Enum

	'Job running status variable
    Protected m_JobRunning As Boolean

    Public Sub New()

    End Sub

    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)
        m_ICR2LSObj.DebugLevel = m_DebugLevel

    End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

        'Get the settings file info via the base class
		If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		'Remainder of tasks are in subclass

	End Function

	Protected MustOverride Function DeleteDataFile() As IJobParams.CloseOutType

    Protected Overridable Function PerfPostAnalysisTasks() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum)
        End If

        'Get rid of raw data file
        result = DeleteDataFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Error deleting raw files; the error will have already been logged
            ' Since the results might still be good, we will not return an error at this point
        End If

        'make results folder
        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
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

    End Function

	Protected Function WaitForJobToFinish() As Boolean
        Const LOGGER_DETAILED_STATUS_INTERVAL_SECONDS As Integer = 120
        Dim dtLastLogTime As System.DateTime

		'Waits for ICR2LS job to finish after being started by subclass call
		Dim StatusResult As ICR_STATUS
		'		Dim Progress As Single

        'Monitor status
        dtLastLogTime = System.DateTime.Now()

		While m_JobRunning
            System.Threading.Thread.Sleep(4000)         'Delay for 4 seconds
            StatusResult = CType(m_ICR2LSObj.Status, ICR_STATUS)

            Select Case StatusResult
                'TODO: Update this statement when new ICR2LS states are added
                Case ICR_STATUS.STATE_PROCESSING, ICR_STATUS.STATE_GENERATING, ICR_STATUS.STATE_QTOFPEKGENERATION, _
                     ICR_STATUS.STATE_LCQTICGENERATION, ICR_STATUS.STATE_MMTOFPEKGENERATION, _
                     ICR_STATUS.STATE_LTQFTPEKGENERATION, ICR_STATUS.STATE_TICGENERATION

                    'Report progress
                    m_progress = m_ICR2LSObj.Progress

                    'Update the status report
                    m_StatusTools.UpdateAndWrite(m_progress)

                    If m_DebugLevel >= 2 Then
                        If System.DateTime.Now.Subtract(dtLastLogTime).TotalSeconds >= LOGGER_DETAILED_STATUS_INTERVAL_SECONDS Then
                            dtLastLogTime = System.DateTime.Now
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); " & _
                                               "StatusResult=" & StatusResult.ToString & "; " & _
                                               "Progess=" & m_progress.ToString("0.00"))
                        End If
                    End If

                Case Else
                    'Analysis is no longer running, exit loop
                    m_JobRunning = False
                    If m_DebugLevel > 0 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Ending loop")
                    End If
            End Select
        End While

		'Close the ICR2LS object
		If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Closing ICR2LS object")
        End If
		m_ICR2LSObj.CloseICR2LS()
		m_ICR2LSObj = Nothing
		'Fire off the garbage collector to make sure ICR2LS dies
		GC.Collect()
		GC.WaitForPendingFinalizers()
		'Delay to allow ICR2LS to close everything
		System.Threading.Thread.Sleep(3000)

		'Verify ICR2LS exited due to job completion
		If StatusResult <> ICR_STATUS.STATE_FINISHED Then
			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); State<>FINISHED")
            End If
			Return False
		Else
			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); State=FINISHED")
            End If
			Return True
		End If

	End Function

End Class
