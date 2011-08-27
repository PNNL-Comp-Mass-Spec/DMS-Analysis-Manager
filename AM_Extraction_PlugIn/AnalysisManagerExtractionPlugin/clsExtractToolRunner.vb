'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/10/2008
'
' Last modified 10/24/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************
Imports AnalysisManagerBase

Public Class clsExtractToolRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Primary class for controlling data extraction
	'*********************************************************************************************************

#Region "Constants"
    Protected Const SEQUEST_PROGRESS_EXTRACTION_DONE As Single = 33
    Protected Const SEQUEST_PROGRESS_PHRP_DONE As Single = 66
    Protected Const SEQUEST_PROGRESS_PEPPROPHET_DONE As Single = 100

    Public Const INSPECT_UNFILTERED_RESULTS_FILE_SUFFIX As String = "_inspect_unfiltered.txt"

#End Region

#Region "Module variables"
    Protected WithEvents m_PeptideProphet As clsPeptideProphetWrapper
    Protected WithEvents m_PHRP As clsPepHitResultsProcWrapper
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Runs the data extraction tool(s)
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As AnalysisManagerBase.IJobParams.CloseOutType

		Dim Msg As String = ""
        Dim Result As IJobParams.CloseOutType
        Dim eReturnCode As IJobParams.CloseOutType

        Dim blnProcessingError As Boolean

        Try

            'Call base class for initial setup
            MyBase.RunTool()

            ' Store the AnalysisManager version info in the database
            StoreToolVersionInfo()

            ' Make sure clsGlobal.m_Completions_Msg is empty
            clsGlobal.m_Completions_Msg = String.Empty

            Select Case m_jobParams.GetParam("ResultType")
                Case "Peptide_Hit"  'Sequest result type
                    'Run Ken's Peptide Extractor DLL
                    Result = PerformPeptideExtraction()
                    'Check for no data first. If no data, then exit but still copy results to server
                    If Result = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
                        Exit Select
                    End If
                    If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Msg = "Error running peptide extraction for sequest"
                        m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                        blnProcessingError = True
                    Else
                        m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE     ' 33% done
                        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
                    End If

                    'Run PHRP
                    Result = RunPhrpForSequest()
                    If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Msg = "Error running peptide hits result processor for sequest"
                        m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                        blnProcessingError = True
                    Else
                        m_progress = SEQUEST_PROGRESS_PHRP_DONE     ' 66% done
                        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
                    End If

                    'Run PeptideProphet
                    Result = RunPeptideProphet()
                    If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Msg = "Error running peptide prophet for sequest"
                        m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                        blnProcessingError = True
                    Else
                        m_progress = SEQUEST_PROGRESS_PEPPROPHET_DONE     ' 100% done
                        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
                    End If

                Case "XT_Peptide_Hit" 'XTandem result type
                    'Run PHRP
                    Result = RunPhrpForXTandem()
                    If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Msg = "Error running peptide hits result processor for Xtandem"
                        m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                        blnProcessingError = True
                    Else
                        m_progress = 100    ' 100% done
                        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
                    End If

                Case "IN_Peptide_Hit"
                    'Run PHRP
                    Result = RunPhrpForInSpecT()
                    If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Msg = "Error running peptide hits result processor for inspect"
                        m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                        blnProcessingError = True
                    Else
                        m_progress = 100    ' 100% done
                        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
                    End If

                Case "MSG_Peptide_Hit"
                    'Run PHRP
                    Result = RunPhrpForMSGFDB()
                    If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Msg = "Error running peptide hits result processor for MSGF-DB"
                        m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                        blnProcessingError = True
                    Else
                        m_progress = 100    ' 100% done
                        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
                    End If

                Case Else
                    'Should never get here - invalid result type specified
                    Msg = "Invalid ResultType specified: " & m_jobParams.GetParam("ResultType")
                    m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Select

            'Stop the job timer
            m_StopTime = System.DateTime.Now

            If blnProcessingError Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the Result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            Result = MakeResultsFolder()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Result = MoveResultFiles()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MoveResultFiles moves the Result files to the Result folder
                m_message = "Error moving files into results folder"
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                ' Try to save whatever files were moved into the results folder
                Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Result = CopyResultsFolderToServer()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return Result
            End If

        Catch ex As System.Exception
            Msg = "clsExtractToolRunner.RunTool(); Exception running extraction tool: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running extraction tool")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'If we got to here, everything worked so exit happily
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Calls Ken's DLL to perform peptide hit extraction for Sequest data
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Private Function PerformPeptideExtraction() As IJobParams.CloseOutType

		Dim Msg As String = ""
		Dim Result As IJobParams.CloseOutType
        Dim PepExtractTool As New clsPeptideExtractWrapper(m_mgrParams, m_jobParams, m_StatusTools)

		'Run the extractor
		If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsExtractToolRunner.PerformPeptideExtraction(); Starting peptide extraction")
        End If
		Try
			Result = PepExtractTool.PerformExtraction
		Catch ex As System.Exception
			Msg = "clsExtractToolRunner.PerformPeptideExtraction(); Exception running extraction tool: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running extraction tool")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

		If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) And _
		  (Result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA) Then
            'log error and return result calling routine handles the error appropriately
            Msg = "Error encoutered during extraction"
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.PerformPeptideExtraction(); " & Msg)
            Return Result
        End If

		'If there was a _syn.txt file created, but it contains no data, then we want to clean up and exit
		If Result = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
            'log error and return result calling routine handles the error appropriately
            clsGlobal.m_Completions_Msg = "No results above threshold"
            Msg = "No results above threshold"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return Result
        End If

	End Function

	''' <summary>
	''' Runs PeptideHitsResultsProcessor on Sequest output
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Private Function RunPhrpForSequest() As IJobParams.CloseOutType

		Dim Msg As String = ""
        Dim Result As IJobParams.CloseOutType

        m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

		'Run the processor
		If m_DebugLevel > 3 Then
            Msg = "clsExtractToolRunner.RunPhrpForSequest(); Starting PHRP"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
        End If
        Try
            Dim strTargetFilePath As String = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_syn.txt")
            Result = m_PHRP.ExtractDataFromResults(strTargetFilePath)
        Catch ex As System.Exception
            Msg = "clsExtractToolRunner.RunPhrpForSequest(); Exception running PHRP: " & _
             ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running PHRP")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
            Msg = "Error running PHRP"
            If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function RunPhrpForXTandem() As IJobParams.CloseOutType

        Dim Msg As String = ""
        Dim Result As IJobParams.CloseOutType

        m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

        'Run the processor
        If m_DebugLevel > 2 Then
            Msg = "clsExtractToolRunner.RunPhrpForXTandem(); Starting PHRP"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
        End If
        Try
            Dim strTargetFilePath As String = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_xt.xml")
            Result = m_PHRP.ExtractDataFromResults(strTargetFilePath)
        Catch ex As System.Exception
            Msg = "clsExtractToolRunner.RunPhrpForXTandem(); Exception running PHRP: " & _
             ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running PHRP")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
            Msg = "Error running PHRP"
            If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function RunPhrpForMSGFDB() As IJobParams.CloseOutType

        Dim Msg As String = ""

        Dim CreateMSGFDBFirstHitsFile As Boolean
        Dim CreateMSGFDBSynopsisFile As Boolean

        Dim strTargetFilePath As String

        Dim blnSuccess As Boolean
        Dim Result As IJobParams.CloseOutType

        m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

        'Run the processor
        If m_DebugLevel > 3 Then
            Msg = "clsExtractToolRunner.RunPhrpForMSGFDB(); Starting PHRP"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
        End If

        Try
            ' The goal:
            '   Create the _fht.txt and _syn.txt files from the _msgfdb.txt file in the _msgfdb.zip file

            ' Extract _msgfdb.txt from the _msgfdb.zip file
            strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_msgfdb.zip")
            blnSuccess = MyBase.UnzipFile(strTargetFilePath)

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Create the First Hits files using the _msgfdb.txt file
            CreateMSGFDBFirstHitsFile = True
            CreateMSGFDBSynopsisFile = True
            strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_msgfdb.txt")
            Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateMSGFDBFirstHitsFile, CreateMSGFDBSynopsisFile)

            If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
                Msg = "Error running PHRP"
                If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Try
                ' Delete the _msgfdb.txt file
                System.IO.File.Delete(strTargetFilePath)
            Catch ex As System.Exception
                ' Ignore errors here
            End Try

        Catch ex As System.Exception
            Msg = "clsExtractToolRunner.RunPhrpForMSGFDB(); Exception running PHRP: " & _
             ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running PHRP")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function RunPhrpForInSpecT() As IJobParams.CloseOutType

        Dim Msg As String = ""

        Dim CreateInspectFirstHitsFile As Boolean
        Dim CreateInspectSynopsisFile As Boolean

        Dim strTargetFilePath As String

        Dim blnSuccess As Boolean
        Dim Result As IJobParams.CloseOutType

        m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

        'Run the processor
        If m_DebugLevel > 3 Then
            Msg = "clsExtractToolRunner.RunPhrpForInSpecT(); Starting PHRP"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
        End If

        Try
            ' The goal:
            '   Get the _fht.txt and _FScore_fht.txt files from the _inspect.txt file in the _inspect_fht.zip file
            '   Get the other files from the _inspect.txt file in the_inspect.zip file

            ' Extract _inspect.txt from the _inspect_fht.zip file
            strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect_fht.zip")
            blnSuccess = MyBase.UnzipFile(strTargetFilePath)

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Create the First Hits files using the _inspect.txt file
            CreateInspectFirstHitsFile = True
            CreateInspectSynopsisFile = False
            strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect.txt")
            Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateInspectFirstHitsFile, CreateInspectSynopsisFile)

            If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
                Msg = "Error running PHRP"
                If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Delete the _inspect.txt file
            System.IO.File.Delete(strTargetFilePath)

            System.Threading.Thread.Sleep(250)


            ' Extract _inspect.txt from the _inspect.zip file
            strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect.zip")
            blnSuccess = MyBase.UnzipFile(strTargetFilePath)

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Create the Synopsis files using the _inspect.txt file
            CreateInspectFirstHitsFile = False
            CreateInspectSynopsisFile = True
            strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect.txt")
            Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateInspectFirstHitsFile, CreateInspectSynopsisFile)

            If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
                Msg = "Error running PHRP"
                If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Try
                ' Delete the _inspect.txt file
                System.IO.File.Delete(strTargetFilePath)
            Catch ex As System.Exception
                ' Ignore errors here
            End Try

        Catch ex As System.Exception
            Msg = "clsExtractToolRunner.RunPhrpForInSpecT(); Exception running PHRP: " & _
             ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception running PHRP")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RunPeptideProphet() As IJobParams.CloseOutType
        Const SYN_FILE_MAX_SIZE_MB As Integer = 200
        Const PEPPROPHET_RESULT_FILE_SUFFIX As String = "_PepProphet.txt"

        Dim Msg As String
        Dim fiSynFile As System.IO.FileInfo

        Dim SynFile As String
        Dim strFileList() As String
        Dim strBaseName As String
        Dim strSynFileNameAndSize As String

        Dim strPepProphetOutputFilePath As String

        Dim eResult As IJobParams.CloseOutType
        Dim blnIgnorePeptideProphetErrors As Boolean

        Dim intFileIndex As Integer
        Dim sngParentSynFileSizeMB As Single
        Dim blnSuccess As Boolean

        blnIgnorePeptideProphetErrors = AnalysisManagerBase.clsGlobal.CBoolSafe(m_jobParams.GetParam("IgnorePeptideProphetErrors"))

        Dim progLoc As String = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc")

        ' verify that program file exists
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then
                m_message = "Manager parameter PeptideProphetRunnerProgLoc is not defined in the Manager Control DB"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Else
                m_message = "Cannot find PeptideProphetRunner program file"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        m_PeptideProphet = New clsPeptideProphetWrapper(progLoc)

        If m_DebugLevel >= 3 Then
            Msg = "clsExtractToolRunner.RunPeptideProphet(); Starting Peptide Prophet"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
        End If

        SynFile = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_syn.txt")

        'Check to see if Syn file exists
        fiSynFile = New System.IO.FileInfo(SynFile)
        If Not fiSynFile.Exists Then
            Msg = "clsExtractToolRunner.RunPeptideProphet(); Syn file " & SynFile & " not found; unable to run peptide prophet"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Check the size of the Syn file
        ' If it is too large, then we will need to break it up into multiple parts, process each part separately, and then combine the results
        sngParentSynFileSizeMB = CSng(fiSynFile.Length / 1024.0 / 1024.0)
        If sngParentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB Then
            ReDim strFileList(0)
            strFileList(0) = fiSynFile.FullName
        Else
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Synopsis file is " & sngParentSynFileSizeMB.ToString("0.0") & " MB, which is larger than the maximum size for peptide prophet (" & SYN_FILE_MAX_SIZE_MB & " MB); splitting into multiple sections")
            End If

            ' File is too large; split it into multiple chunks
            ReDim strFileList(0)
            blnSuccess = SplitFileRoundRobin(fiSynFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, True, strFileList)

            If blnSuccess Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Synopsis file was split into " & strFileList.Length & " sections by SplitFileRoundRobin")
                End If
            Else
                Msg = "Error splitting synopsis file that is over " & SYN_FILE_MAX_SIZE_MB & " MB in size"

                If blnIgnorePeptideProphetErrors Then
                    Msg &= "; Ignoring the error since 'IgnorePeptideProphetErrors' = True"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
                    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            End If
        End If

        'Setup Peptide Prophet and run for each file in strFileList
        For intFileIndex = 0 To strFileList.Length - 1
            m_PeptideProphet.InputFile = strFileList(intFileIndex)
            m_PeptideProphet.Enzyme = "tryptic"
            m_PeptideProphet.OutputFolderPath = m_WorkDir
            m_PeptideProphet.DebugLevel = m_DebugLevel

            fiSynFile = New System.IO.FileInfo(strFileList(intFileIndex))
            strSynFileNameAndSize = fiSynFile.Name & " (file size = " & (fiSynFile.Length / 1024.0 / 1024.0).ToString("0.00") & " MB"
            If strFileList.Length > 1 Then
                strSynFileNameAndSize &= "; parent syn file is " & sngParentSynFileSizeMB.ToString("0.00") & " MB)"
            Else
                strSynFileNameAndSize &= ")"
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running peptide prophet on file " & strSynFileNameAndSize)
            End If

            eResult = m_PeptideProphet.CallPeptideProphet()

            If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then

                ' Make sure the Peptide Prophet output file was actually created
                strPepProphetOutputFilePath = System.IO.Path.Combine(m_PeptideProphet.OutputFolderPath, _
                                                                System.IO.Path.GetFileNameWithoutExtension(strFileList(intFileIndex)) & _
                                                                PEPPROPHET_RESULT_FILE_SUFFIX)

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide prophet processing complete; checking for file " & strPepProphetOutputFilePath)
                End If

                If Not System.IO.File.Exists(strPepProphetOutputFilePath) Then

                    Msg = "clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " & strSynFileNameAndSize
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

                    Msg = m_PeptideProphet.ErrMsg
                    If Msg.Length > 0 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    End If

                    If blnIgnorePeptideProphetErrors Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True")
                    Else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "To ignore this error, update this job to use a settings file that has 'IgnorePeptideProphetErrors' set to True")
                        eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
                        Exit For
                    End If
                End If
            Else
                Msg = "clsExtractToolRunner.RunPeptideProphet(); Error running Peptide Prophet on file " & strSynFileNameAndSize & _
                      ": " & m_PeptideProphet.ErrMsg
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

                If blnIgnorePeptideProphetErrors Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True")
                Else
                    eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
                    Exit For
                End If
            End If

        Next

        If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS OrElse blnIgnorePeptideProphetErrors Then
            If strFileList.Length > 1 Then

                ' Delete each of the temporary synopsis files
                DeleteTemporaryFiles(strFileList)

                ' We now need to recombine the peptide prophet result files

                ' Update strFileList() to have the peptide prophet result file names
                strBaseName = System.IO.Path.Combine(m_PeptideProphet.OutputFolderPath, System.IO.Path.GetFileNameWithoutExtension(SynFile))

                For intFileIndex = 0 To strFileList.Length - 1
                    strFileList(intFileIndex) = strBaseName & "_part" & (intFileIndex + 1).ToString & PEPPROPHET_RESULT_FILE_SUFFIX

                    ' Add this file to the global delete list
                    clsGlobal.FilesToDelete.Add(strFileList(intFileIndex))
                Next intFileIndex

                ' Define the final peptide prophet output file name
                strPepProphetOutputFilePath = strBaseName & PEPPROPHET_RESULT_FILE_SUFFIX

                If m_DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Combining " & strFileList.Length & " separate Peptide Prophet result files to create " & System.IO.Path.GetFileName(strPepProphetOutputFilePath))
                End If

                blnSuccess = InterleaveFiles(strFileList, strPepProphetOutputFilePath, True)

                ' Delete each of the temporary peptide prophet result files
                DeleteTemporaryFiles(strFileList)

                If blnSuccess Then
                    eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                Else
                    Msg = "Error interleaving the peptide prophet result files (FileCount=" & strFileList.Length & ")"
                    If blnIgnorePeptideProphetErrors Then
                        Msg &= "; Ignoring the error since 'IgnorePeptideProphetErrors' = True"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
                        eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                    Else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                        eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                End If
            End If

        End If

        Return eResult

    End Function

    ''' <summary>
    ''' Deletes each file in strFileList()
    ''' </summary>
    ''' <param name="strFileList">Full paths to files to delete</param>
    ''' <remarks></remarks>
    Private Sub DeleteTemporaryFiles(ByVal strFileList() As String)
        Dim intFileIndex As Integer

        System.Threading.Thread.Sleep(2000)                    'Delay for 2 seconds
        GC.Collect()
        GC.WaitForPendingFinalizers()

        ' Delete each file in strFileList
        For intFileIndex = 0 To strFileList.Length - 1
            If m_DebugLevel >= 5 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting file " & strFileList(intFileIndex))
            End If
            Try
                System.IO.File.Delete(strFileList(intFileIndex))
            Catch ex As System.Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting file " & System.IO.Path.GetFileName(strFileList(intFileIndex)) & ": " & ex.Message)
            End Try
        Next intFileIndex

    End Sub

    ''' <summary>
    ''' Reads each file in strFileList() line by line, writing the lines to strCombinedFilePath
    ''' Can also check for a header line on the first line; if a header line is found in the first file,
    ''' then the header is also written to the combined file
    ''' </summary>
    ''' <param name="strFileList">Files to combine</param>
    ''' <param name="strCombinedFilePath">File to create</param>
    ''' <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
    ''' <returns>True if success; false if failure</returns>
    ''' <remarks></remarks>
    Protected Function InterleaveFiles(ByRef strFileList() As String, _
                                       ByVal strCombinedFilePath As String, _
                                       ByVal blnLookForHeaderLine As Boolean) As Boolean

        Dim Msg As String
        Dim intIndex As Integer

        Dim intFileCount As Integer
        Dim srInFiles() As System.IO.StreamReader
        Dim swOutFile As System.IO.StreamWriter

        Dim strLineIn As String = String.Empty
        Dim strSplitLine() As String

        Dim intFileIndex As Integer
        Dim intLinesRead() As Integer
        Dim intTotalLinesRead As Integer

        Dim intTotalLinesReadSaved As Integer

        Dim blnContinueReading As Boolean
        Dim blnProcessLine As Boolean
        Dim blnSuccess As Boolean

        Try
            If strFileList Is Nothing OrElse strFileList.Length = 0 Then
                ' Nothing to do
                Return False
            End If

            intFileCount = strFileList.Length
            ReDim srInFiles(intFileCount - 1)
            ReDim intLinesRead(intFileCount - 1)

            ' Open each of the input files
            For intIndex = 0 To intFileCount - 1
                If System.IO.File.Exists(strFileList(intIndex)) Then
                    srInFiles(intIndex) = New System.IO.StreamReader(New System.IO.FileStream(strFileList(intIndex), System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                Else
                    ' File not found; unable to continue
                    Msg = "Source peptide prophet file not found, unable to continue: " & strFileList(intIndex)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    Return False
                End If
            Next

            ' Create the output file

            swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strCombinedFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

            intTotalLinesRead = 0
            blnContinueReading = True

            Do While blnContinueReading
                intTotalLinesReadSaved = intTotalLinesRead
                For intFileIndex = 0 To intFileCount - 1

                    If srInFiles(intFileIndex).Peek >= 0 Then
                        strLineIn = srInFiles(intFileIndex).ReadLine

                        intLinesRead(intFileIndex) += 1
                        intTotalLinesRead += 1

                        If Not strLineIn Is Nothing Then
                            blnProcessLine = True

                            If intLinesRead(intFileIndex) = 1 AndAlso blnLookForHeaderLine AndAlso strLineIn.Length > 0 Then
                                ' Check for a header line
                                strSplitLine = strLineIn.Split(New Char() {ControlChars.Tab}, 2)

                                If strSplitLine.Length > 0 AndAlso Not Double.TryParse(strSplitLine(0), 0) Then
                                    ' First column does not contain a number; this must be a header line
                                    ' Write the header to the output file (provided intFileIndex=0)
                                    If intFileIndex = 0 Then
                                        swOutFile.WriteLine(strLineIn)
                                    End If
                                    blnProcessLine = False
                                End If
                            End If

                            If blnProcessLine Then
                                swOutFile.WriteLine(strLineIn)
                            End If

                        End If
                    End If

                Next

                If intTotalLinesRead = intTotalLinesReadSaved Then
                    blnContinueReading = False
                End If
            Loop

            ' Close the input files
            For intIndex = 0 To intFileCount - 1
                srInFiles(intIndex).Close()
            Next

            ' Close the output file
            swOutFile.Close()

            blnSuccess = True


        Catch ex As System.Exception
            Msg = "Exception in clsExtractToolRunner.InterleaveFiles: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception in InterleaveFiles")
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Reads strSrcFilePath line-by-line and splits into multiple files such that none of the output 
    ''' files has length greater than lngMaxSizeBytes. Can also check for a header line on the first line;
    ''' if a header line is found, then all of the split files will be assigned the same header line
    ''' </summary>
    ''' <param name="strSrcFilePath">FilePath to parse</param>
    ''' <param name="lngMaxSizeBytes">Maximum size of each file</param>
    ''' <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
    ''' <param name="strSplitFileList">Output array listing the full paths to the split files that were created</param>
    ''' <returns>True if success, false if failure</returns>
    ''' <remarks></remarks>
    Private Function SplitFileRoundRobin(ByVal strSrcFilePath As String, _
                                         ByVal lngMaxSizeBytes As Int64, _
                                         ByVal blnLookForHeaderLine As Boolean, _
                                         ByRef strSplitFileList() As String) As Boolean

        Dim fiFileInfo As System.IO.FileInfo
        Dim strBaseName As String

        Dim intLinesRead As Integer = 0
        Dim intTargetFileIndex As Integer

        Dim Msg As String
        Dim strLineIn As String = String.Empty
        Dim strSplitLine() As String

        Dim srInFile As System.IO.StreamReader
        Dim swOutFiles() As System.IO.StreamWriter

        Dim intSplitCount As Integer
        Dim intIndex As Integer

        Dim blnProcessLine As Boolean
        Dim blnSuccess As Boolean = False

        Try
            fiFileInfo = New System.IO.FileInfo(strSrcFilePath)
            If Not fiFileInfo.Exists Then Return False

            If fiFileInfo.Length <= lngMaxSizeBytes Then
                ' File is already less than the limit
                ReDim strSplitFileList(0)
                strSplitFileList(0) = fiFileInfo.FullName

                blnSuccess = True
            Else

                ' Determine the number of parts to split the file into
                intSplitCount = CInt(Math.Ceiling(fiFileInfo.Length / CDbl(lngMaxSizeBytes)))

                If intSplitCount < 2 Then
                    ' This code should never be reached; we'll set intSplitCount to 2
                    intSplitCount = 2
                End If

                ' Open the input file
                srInFile = New System.IO.StreamReader(New System.IO.FileStream(fiFileInfo.FullName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

                ' Create each of the output files
                ReDim strSplitFileList(intSplitCount - 1)
                ReDim swOutFiles(intSplitCount - 1)

                strBaseName = System.IO.Path.Combine(fiFileInfo.DirectoryName, System.IO.Path.GetFileNameWithoutExtension(fiFileInfo.Name))

                For intIndex = 0 To intSplitCount - 1
                    strSplitFileList(intIndex) = strBaseName & "_part" & (intIndex + 1).ToString & System.IO.Path.GetExtension(fiFileInfo.Name)
                    swOutFiles(intIndex) = New System.IO.StreamWriter(New System.IO.FileStream(strSplitFileList(intIndex), System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))
                Next

                intLinesRead = 0
                intTargetFileIndex = 0

                Do While srInFile.Peek >= 0
                    strLineIn = srInFile.ReadLine
                    intLinesRead += 1

                    If Not strLineIn Is Nothing Then
                        blnProcessLine = True

                        If intLinesRead = 1 AndAlso blnLookForHeaderLine AndAlso strLineIn.Length > 0 Then
                            ' Check for a header line
                            strSplitLine = strLineIn.Split(New Char() {ControlChars.Tab}, 2)

                            If strSplitLine.Length > 0 AndAlso Not Double.TryParse(strSplitLine(0), 0) Then
                                ' First column does not contain a number; this must be a header line
                                ' Write the header to each output file
                                For intIndex = 0 To intSplitCount - 1
                                    swOutFiles(intIndex).WriteLine(strLineIn)
                                Next
                                blnProcessLine = False
                            End If
                        End If

                        If blnProcessLine Then
                            swOutFiles(intTargetFileIndex).WriteLine(strLineIn)
                            intTargetFileIndex += 1
                            If intTargetFileIndex = intSplitCount Then intTargetFileIndex = 0
                        End If
                    End If
                Loop

                ' Close the input file
                srInFile.Close()

                ' Close the output files
                For intIndex = 0 To intSplitCount - 1
                    swOutFiles(intIndex).Close()
                Next

                blnSuccess = True
            End If


        Catch ex As System.Exception
            Msg = "Exception in clsExtractToolRunner.SplitFileRoundRobin: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception in SplitFileRoundRobin")
            blnSuccess = False
        End Try

        Return blnSuccess

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

        ' Lookup the version of the PeptideHitResultsProcessor
        Try

            Dim progLoc As String = m_mgrParams.GetParam("PHRPProgLoc")
            Dim ioPHRP As System.IO.DirectoryInfo
            ioPHRP = New System.IO.DirectoryInfo(progLoc)

            ' verify that program file exists
            If ioPHRP.Exists Then
                Dim oAssemblyName As System.Reflection.AssemblyName
                Dim strDLLPath As String = System.IO.Path.Combine(ioPHRP.FullName, "PeptideHitResultsProcessor.dll")
                oAssemblyName = System.Reflection.Assembly.LoadFrom(strDLLPath).GetName

                Dim strNameAndVersion As String
                strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PHRP folder not found at " & progLoc)
            End If

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for the PeptideHitResultsProcessor: " & ex.Message)
        End Try


        If m_jobParams.GetParam("ResultType") = "Peptide_Hit" Then
            'Sequest result type

            ' Lookup the version of the PeptideFileExtractor
            Try
                Dim oAssemblyName As System.Reflection.AssemblyName
                oAssemblyName = System.Reflection.Assembly.Load("PeptideFileExtractor").GetName

                Dim strNameAndVersion As String
                strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

            Catch ex As System.Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for the PeptideFileExtractor: " & ex.Message)
            End Try

            ' Lookup the version of the PeptideProphetRunner

            Dim strPeptideProphetRunnerLoc As String = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc")
            Dim ioPeptideProphetRunner As System.IO.FileInfo = New System.IO.FileInfo(strPeptideProphetRunnerLoc)

            If ioPeptideProphetRunner.Exists() Then
                ' Lookup the version of the PeptideProphetRunner
                Try
                    Dim oAssemblyName As System.Reflection.AssemblyName
                    oAssemblyName = System.Reflection.Assembly.LoadFrom(ioPeptideProphetRunner.FullName).GetName

                    Dim strNameAndVersion As String
                    strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
                    strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

                Catch ex As System.Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for the PeptideProphetRunner: " & ex.Message)
                End Try

                ' Lookup the version of the PeptideProphetLibrary
                Try
                    Dim oAssemblyName As System.Reflection.AssemblyName
                    Dim strDLLPath As String = System.IO.Path.Combine(ioPeptideProphetRunner.DirectoryName, "PeptideProphetLibrary.dll")
                    oAssemblyName = System.Reflection.Assembly.LoadFrom(strDLLPath).GetName

                    Dim strNameAndVersion As String
                    strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
                    strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

                Catch ex As System.Exception
                    ' If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
                    '  <startup useLegacyV2RuntimeActivationPolicy="true">
                    '    <supportedRuntime version="v4.0" />
                    '  </startup>
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for the PeptideProphetLibrary: " & ex.Message)
                End Try
            End If


        End If

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

#Region "Event handlers"
    Private Sub m_PeptideProphet_PeptideProphetRunning(ByVal PepProphetStatus As String, ByVal PercentComplete As Single) Handles m_PeptideProphet.PeptideProphetRunning
        Const PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS As Integer = 60
        Static dtLastPepProphetStatusLog As System.DateTime = System.DateTime.Now.Subtract(New System.TimeSpan(0, 0, PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS * 2))

        m_progress = SEQUEST_PROGRESS_PHRP_DONE + CSng(PercentComplete / 3.0)
        m_StatusTools.UpdateAndWrite(m_progress)

        If m_DebugLevel >= 4 Then
            If System.DateTime.Now.Subtract(dtLastPepProphetStatusLog).TotalSeconds >= PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS Then
                dtLastPepProphetStatusLog = System.DateTime.Now
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running peptide prophet: " & PepProphetStatus & "; " & PercentComplete & "% complete")
            End If
        End If
    End Sub

    Private Sub m_PHRP_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles m_PHRP.ProgressChanged
        Const PHRP_LOG_INTERVAL_SECONDS As Integer = 180
        Const PHRP_DETAILED_LOG_INTERVAL_SECONDS As Integer = 20

        Static dtLastPHRPStatusLog As System.DateTime = System.DateTime.Now.Subtract(New System.TimeSpan(0, 0, PHRP_DETAILED_LOG_INTERVAL_SECONDS * 2))

        m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE + CSng(percentComplete / 3.0)
        m_StatusTools.UpdateAndWrite(m_progress)

        If m_DebugLevel >= 1 Then
            If System.DateTime.Now.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_DETAILED_LOG_INTERVAL_SECONDS And m_DebugLevel >= 3 OrElse _
               System.DateTime.Now.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_LOG_INTERVAL_SECONDS Then
                dtLastPHRPStatusLog = System.DateTime.Now
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running PHRP: " & taskDescription & "; " & percentComplete & "% complete")
            End If
        End If
    End Sub
#End Region

   
End Class
