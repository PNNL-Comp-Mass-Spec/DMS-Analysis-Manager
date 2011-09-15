'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 09/19/2008
'*********************************************************************************************************

Option Strict On

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

        Dim blnProcessingError As Boolean = False
        Dim Result As IJobParams.CloseOutType
        Dim eReturnCode As IJobParams.CloseOutType

        ' Create some dummy results files
        Dim strSubFolderPath As String

        CreateTestFiles(m_WorkDir, 5, "TestResultFile")

        ' Make some subfolders with more files
        strSubFolderPath = System.IO.Path.Combine(m_WorkDir, "Plots")
        System.IO.Directory.CreateDirectory(strSubFolderPath)
        CreateTestFiles(strSubFolderPath, 4, "Plot")

        strSubFolderPath = System.IO.Path.Combine(strSubFolderPath, "MoreStuff")
        System.IO.Directory.CreateDirectory(strSubFolderPath)
        CreateTestFiles(strSubFolderPath, 5, "Stuff")


        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow

        If blnProcessingError Then
            ' Something went wrong
            ' In order to help diagnose things, we will move whatever files were created into the result folder, 
            '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        Result = MakeResultsFolder()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Result = MoveResultFiles()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MoveResultFiles moves the result files to the result folder
            m_message = "Error moving files into results folder"
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        ' Move the Plots folder to the result files folder
        Dim diPlotsFolder As System.IO.DirectoryInfo
        diPlotsFolder = New System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"))

        Dim strTargetFolderPath As String
        strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots")
        diPlotsFolder.MoveTo(strTargetFolderPath)


        If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Result = CopyResultsFolderToServer()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return Result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

    Private Sub CreateTestFiles(ByVal strFolderPath As String, ByVal intFilesToCreate As Integer, ByVal strFileNameBase As String)

        Dim swOutFile As System.IO.StreamWriter
        Dim objRand As System.Random = New System.Random()
        Dim strOutFilePath As String

        For intIndex As Integer = 1 To intFilesToCreate
            strOutFilePath = System.IO.Path.Combine(strFolderPath, strFileNameBase & intIndex.ToString & "_" & objRand.Next(1, 99) & ".txt")

            swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strOutFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swOutFile.WriteLine(System.DateTime.Now().ToString & " - This is a test file.")
            swOutFile.Close()

            System.Threading.Thread.Sleep(50)
        Next

    End Sub
#End Region

End Class
