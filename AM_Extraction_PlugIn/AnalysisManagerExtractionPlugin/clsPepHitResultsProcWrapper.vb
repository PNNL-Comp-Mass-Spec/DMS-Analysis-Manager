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
#End Region

#Region "Properties"
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
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
	Public Function ExtractDataFromResults() As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType

        result = MakeTextOutputFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Return result

    End Function

    ''' <summary>
    ''' Makes flat text file from output file
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function MakeTextOutputFiles() As IJobParams.CloseOutType

        Dim PeptideSearchResultsFileName As String = String.Empty       'DLL auto-determines file name if this is empty or NULL
        Dim Msg As String = ""
        Dim objPeptideHitResultsProcessor As PeptideHitResultsProcessor.IPeptideHitResultsProcessor
        Dim ModDefsFileName As String
        Dim ParamFileName As String = m_JobParams.GetParam("ParmFileName")

        Try
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

                '                .Logger = m_Logger
                .MiscParams = New System.Collections.Specialized.StringDictionary       ' Empty, since unused
                .OutputFolderPath = m_MgrParams.GetParam("workdir")
                .SourceFolderPath = m_MgrParams.GetParam("workdir")
                .PeptideHitResultsFileName = PeptideSearchResultsFileName
                .MassCorrectionTagsFileName = clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME
                .ModificationDefinitionsFileName = ModDefsFileName
            End With

            objPeptideHitResultsProcessor = New PeptideHitResultsProcessor.clsAnalysisManagerPeptideHitResultsProcessor
            objPeptideHitResultsProcessor.Setup(SetupParams)

        Catch ex As System.Exception
            Msg = "Error initializing the peptide hit results processor, job " & m_JobParams.GetParam("Job")
			Msg &= " , Step " & m_JobParams.GetParam("Step") & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


        'Start the peptide hit results processor
        Try
            Dim RetVal As PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus
            RetVal = objPeptideHitResultsProcessor.Start()
            If RetVal = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_ERROR Then
                Msg = "Error starting spectra processor: " & objPeptideHitResultsProcessor.ErrMsg
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Loop until the results processor finishes
            Do While (objPeptideHitResultsProcessor.Status = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_RUNNING) OrElse _
             (objPeptideHitResultsProcessor.Status = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_STARTING)
                System.Threading.Thread.Sleep(2000)                 'Delay for 2 seconds
            Loop
            RetVal = Nothing
            System.Threading.Thread.Sleep(10000)                    'Delay for 10 seconds
            GC.Collect()
            GC.WaitForPendingFinalizers()
        Catch ex As System.Exception
			Msg = "Exception while running the peptide hit results processor: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Check for reason peptide hit results processor exited
        If objPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_FAILURE Then
            Msg = "Error calling the peptide hit results processor: " & objPeptideHitResultsProcessor.ErrMsg

            ' Truncate the message if longer than 500 characters (the full message has likely already been logged)
            If Msg.Length > 500 Then
                Msg = Msg.Substring(0, 500) & "..."
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        ElseIf objPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_ABORTED Then
            Msg = "Peptide Hit Results Processing aborted"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Delete the Mass Correction Tags file since no longer needed
        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_MgrParams.GetParam("workdir"), _
                   clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME))
        Catch ex As System.Exception
            ' Ignore errors here
        End Try

        'Return results
        If objPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_SUCCESS Then
            objPeptideHitResultsProcessor = Nothing
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

    End Function
#End Region

End Class
