
'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 09/19/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports System.Text.RegularExpressions
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal

Public Class clsCodeTestAM
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Base class for Sequest analysis
	'*********************************************************************************************************

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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.Setup()")
        End If
    End Sub

	''' <summary>
	''' Runs the analysis tool
	''' </summary>
	''' <returns>IJobParams.CloseOutType value indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As AnalysisManagerBase.IJobParams.CloseOutType

		Dim Result As IJobParams.CloseOutType

        ' Create some dummy results files
        Dim swOutFile As System.IO.StreamWriter
        Dim objRand As System.Random = New System.Random()
        Dim strOutFilePath As String

        For intIndex As Integer = 1 To 5
            strOutFilePath = System.IO.Path.Combine(m_WorkDir, "TestResultFile" & intIndex.ToString & "_" & objRand.Next(1, 99) & ".txt")

            swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strOutFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swOutFile.WriteLine(System.DateTime.Now().ToString & "This is a test file.")
            swOutFile.Close()

            System.Threading.Thread.Sleep(250)
        Next

        Result = MakeResultsFolder()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return Result
        End If

        Result = MoveResultFiles()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return Result
        End If

        Result = CopyResultsFolderToServer()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return Result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#End Region

End Class
