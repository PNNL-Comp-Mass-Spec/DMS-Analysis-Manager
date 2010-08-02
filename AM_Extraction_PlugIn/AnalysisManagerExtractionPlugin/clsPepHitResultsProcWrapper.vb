'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 07/11/2007
'
' Program converted from original version written by J.D. Sandoval, PNNL.
' Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
'
' Last modified 01/07/2009
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports AnalysisManagerBase

Public Class clsPepHitResultsProcWrapper

	'*********************************************************************************************************
	' Wraps PeptideHitResultsProcessor.dll
	'*********************************************************************************************************

#Region "Module Variables"
    Private m_DebugLevel As Integer = 0
	Private m_MgrParams As IMgrParams
    Private m_JobParams As IJobParams

    Private WithEvents mPeptideHitResultsProcessor As PeptideHitResultsProcessor.IPeptideHitResultsProcessor

#End Region

#Region "Events"
    Public Event ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
    ''' <param name="MgrParams">IMgrParams object containing manager settings</param>
	''' <param name="JobParams">IJobParams object containing job parameters</param>
	''' <remarks></remarks>
    Public Sub New(ByVal MgrParams As IMgrParams, ByVal JobParams As IJobParams)

        m_MgrParams = MgrParams
        m_JobParams = JobParams
        m_DebugLevel = CInt(m_MgrParams.GetParam("debuglevel"))

    End Sub

    ''' <summary>
    ''' Converts Sequest, X!Tandem, or Inspect output file to a flat file
    ''' Will auto-determine the input file name, and will set CreateFHTFile and CreateSYNFile to True
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function ExtractDataFromResults() As IJobParams.CloseOutType
        '  Let the DLL auto-determines the input filename, based on the dataset name
        Return ExtractDataFromResults(String.Empty, True, True)
    End Function

    ''' <summary>
    ''' Converts Sequest, X!Tandem, or Inspect output file to a flat file
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function ExtractDataFromResults(ByVal PeptideSearchResultsFileName As String, _
                                           ByVal CreateFHTFile As Boolean, _
                                           ByVal CreateSYNFile As Boolean) As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        result = MakeTextOutputFiles(PeptideSearchResultsFileName, CreateFHTFile, CreateSYNFile)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Return result

    End Function

    ''' <summary>
    ''' Makes flat text file from PeptideSearchResultsFileName
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function MakeTextOutputFiles(ByVal PeptideSearchResultsFileName As String, _
                                         ByVal CreateFHTFile As Boolean, _
                                         ByVal CreateSYNFile As Boolean) As IJobParams.CloseOutType

        Dim Msg As String = ""
        Dim ModDefsFileName As String
        Dim ParamFileName As String = m_JobParams.GetParam("ParmFileName")

        Try
            If PeptideSearchResultsFileName Is Nothing Then PeptideSearchResultsFileName = String.Empty

            ' Define the modification definitions file name
            ModDefsFileName = System.IO.Path.GetFileNameWithoutExtension(ParamFileName) & clsAnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX

            'Initialize the plugin
            Dim SetupParams As New PeptideHitResultsProcessor.IPeptideHitResultsProcessor.InitializationParams
            With SetupParams
                .DebugLevel = m_DebugLevel
                .AnalysisToolName = m_JobParams.GetParam("StepTool")
                .DatasetName = m_JobParams.GetParam("DatasetNum")

                ' Set .ParameterFileName to the parameter file used by the peptide search tool (Sequest, XTandem, or Inspect)
                .ParameterFileName = ParamFileName
                .SettingsFileName = m_JobParams.GetParam("genJobParamsFilename")        'Settings File parameters for PHRP are in this file (section PeptideHitResultsProcessorOptions)

                .MiscParams = New System.Collections.Specialized.StringDictionary       ' Empty, since unused
                .OutputFolderPath = m_MgrParams.GetParam("workdir")
                .SourceFolderPath = m_MgrParams.GetParam("workdir")

                '  The DLL will auto-determines the input filename if PeptideSearchResultsFileName is empty
                .PeptideHitResultsFileName = PeptideSearchResultsFileName
                .MassCorrectionTagsFileName = clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME
                .ModificationDefinitionsFileName = ModDefsFileName

                .CreateInspectFirstHitsFile = CreateFHTFile
                .CreateInspectSynopsisFile = CreateSYNFile
            End With

            mPeptideHitResultsProcessor = New PeptideHitResultsProcessor.clsAnalysisManagerPeptideHitResultsProcessor
            mPeptideHitResultsProcessor.Setup(SetupParams)

        Catch ex As System.Exception
            Msg = "Error initializing the peptide hit results processor, job " & m_JobParams.GetParam("Job")
            Msg &= " , Step " & m_JobParams.GetParam("Step") & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


        'Start the peptide hit results processor
        Try
            If m_DebugLevel >= 3 Then
                Msg = "clsPepHitResultsProcWrapper.MakeTextOutputFiles(); Analyzing " & PeptideSearchResultsFileName & " with PHRP"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
            End If

            Dim RetVal As PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus
            RetVal = mPeptideHitResultsProcessor.Start()
            If RetVal = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_ERROR Then
                Msg = "Error starting spectra processor: " & mPeptideHitResultsProcessor.ErrMsg
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Loop until the results processor finishes
            Do While (mPeptideHitResultsProcessor.Status = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_RUNNING) OrElse _
             (mPeptideHitResultsProcessor.Status = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_STARTING)
                System.Threading.Thread.Sleep(2000)                 'Delay for 2 seconds
            Loop
            RetVal = Nothing
            System.Threading.Thread.Sleep(2000)                    'Delay for 2 seconds
            GC.Collect()
            GC.WaitForPendingFinalizers()
        Catch ex As System.Exception
            Msg = "Exception while running the peptide hit results processor: " & _
             ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Check for reason peptide hit results processor exited
        If mPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_FAILURE Then
            Msg = "Error calling the peptide hit results processor: " & mPeptideHitResultsProcessor.ErrMsg

            ' Truncate the message if longer than 500 characters (the full message has likely already been logged)
            If Msg.Length > 500 Then
                Msg = Msg.Substring(0, 500) & "..."
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        ElseIf mPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_ABORTED Then
            Msg = "Peptide Hit Results Processing aborted"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Return results
        If mPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_SUCCESS Then
            mPeptideHitResultsProcessor = Nothing

            If m_DebugLevel >= 3 Then
                Msg = "clsPepHitResultsProcWrapper.MakeTextOutputFiles(); Peptide hit results processor complete"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            Msg = "clsPepHitResultsProcWrapper.MakeTextOutputFiles(); Peptide hit results processor ended with an unknown state, " & mPeptideHitResultsProcessor.Results.ToString

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
        
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

    End Function
#End Region

#Region "Event Handlers"

    Private Sub mPeptideHitResultsProcessor_DebugEvent(ByVal strMessage As String) Handles mPeptideHitResultsProcessor.DebugEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
    End Sub

    Private Sub mPeptideHitResultsProcessor_ErrorOccurred(ByVal strMessage As String) Handles mPeptideHitResultsProcessor.ErrorOccurred
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PHRP error: " & strMessage)
    End Sub

    Private Sub mPeptideHitResultsProcessor_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mPeptideHitResultsProcessor.ProgressChanged
        RaiseEvent ProgressChanged(taskDescription, percentComplete)
    End Sub
#End Region
End Class
