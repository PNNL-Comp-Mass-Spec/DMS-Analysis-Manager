'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/29/2008
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase

Public Class clsDtaGenToolRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Base class for DTA generation tool runners
	'*********************************************************************************************************

#Region "Module variables"
	Dim WithEvents m_CmdRunner As clsRunDosProgram
#End Region

#Region "Methods"
	''' <summary>
	''' Runs the analysis tool
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks>This method is used to meet the interface requirement</remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType

		'do the stuff in the base class
		If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		'implement master MSMS analysis tool sequence

		'Create spectra files
		result = CreateMSMSSpectra()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return result

        'Stop the job timer
		m_StopTime = Now

        'Add the current job data to the summary file
        Try
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End Try

        'Get rid of raw data file
        result = DeleteDataFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files

        'Add any files that are an exception to the captured files to delete list
        clsGlobal.m_ExceptionFiles.add("lcq_dta.txt")

        Dim ext As String
        Dim DumFiles() As String

        'update list of files to be deleted after run
        For Each ext In clsGlobal.m_FilesToDeleteExt
            DumFiles = System.IO.Directory.GetFiles(m_mgrParams.GetParam("workdir"), "*" & ext) 'Zipped DTA
            For Each FileToDel As String In DumFiles
                clsGlobal.FilesToDelete.Add(FileToDel)
            Next
        Next

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
            '    'TODO: What do we do here?
            Return result
        End If

        If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Make results folder and transfer results
        'result = DispositionResults()
        'If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return result

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

	''' <summary>
	''' Creates DTA files and filters if necessary
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overridable Function CreateMSMSSpectra() As IJobParams.CloseOutType

		'Make the spectra files
		Dim Result As IJobParams.CloseOutType = MakeSpectraFiles()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

        'Concatenate spectra files
		Result = ConcatSpectraFiles()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

		'Zip concatenated spectra files
		Result = ZipConcDtaFile()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

		'If we got to here, everything's OK
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

    ''' <summary>
    ''' Detailed method for running a tool
    ''' </summary>
    ''' <returns>CloseoutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
	Public Overridable Function DispositionResults() As IJobParams.CloseOutType

		Dim StepResult As IJobParams.CloseOutType

		'Make sure all files have released locks
		GC.Collect()
		GC.WaitForPendingFinalizers()
        System.Threading.Thread.Sleep(1000)

        'Get rid of raw data file
		Try
			StepResult = DeleteDataFile()
			If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return StepResult
			End If
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.DispositionResults(), Exception while deleting data file, " & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Add the current job data to the summary file
		Try
			If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End Try

		'Delete .dta files
		Dim TmpFile As String
		Dim FileList() As String
		Try
            FileList = System.IO.Directory.GetFiles(m_WorkDir, "*.dta")
			For Each TmpFile In FileList
				DeleteFileWithRetries(TmpFile)
			Next
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting .dta files, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & "; " & Err.Message)
            m_message = AppendToComment(m_message, "Error deleting .dta files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Delete unzipped concatenated dta files
        FileList = System.IO.Directory.GetFiles(m_WorkDir, "*_dta.txt")
		For Each TmpFile In FileList
			Try
                If System.IO.Path.GetFileName(TmpFile.ToLower) <> "lcq_dta.txt" Then
                    DeleteFileWithRetries(TmpFile)
                End If
			Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error: " & ex.Message & " deleting concatenated dta file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & "; " & ex.Message)
                m_message = AppendToComment(m_message, "Error packaging results")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		Next

		'make results folder
		Try
			StepResult = MakeResultsFolder()
			If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return StepResult
			End If
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.DispositionResults()(), Exception making results folder, " & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Copy results folder to storage server
		Try
			StepResult = CopyResultsFolderToServer
			If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return StepResult
			End If
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.DispositionResults(), Exception moving results folder, " & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	''' <summary>
	''' Creates DTA files
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overridable Function MakeSpectraFiles() As IJobParams.CloseOutType

        'Make individual spectra files from input raw data file, using plugin

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Making spectra files, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))

		'Load the appropriate plugin
		Dim SpectraGen As ISpectraFileProcessor = clsPluginLoader.GetSpectraGenerator(m_jobParams.GetParam("rawdatatype"))
		If SpectraGen Is Nothing Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.MakeSpectraFiles: Error loading spectra processor: " & clsPluginLoader.Message)
            m_message = AppendToComment(m_message, "Error loading spectra processor")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.MakeSpectraFiles: Loaded spectra generator " & clsPluginLoader.Message)
        End If

		'Initialize the plugin
		Dim SetupParams As ISpectraFileProcessor.InitializationParams
		With SetupParams
			.DebugLevel = m_DebugLevel
			.JobParams = m_jobParams
            .MgrParams = m_mgrParams
            .StatusTools = m_StatusTools
		End With
		SpectraGen.Setup(SetupParams)

		'Start the spectra generation process
		Try
			Dim RetVal As ISpectraFileProcessor.ProcessStatus = SpectraGen.Start
            If RetVal = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.MakeSpectraFiles: Error starting spectra processor: " & SpectraGen.ErrMsg)
                m_message = AppendToComment(m_message, "Error starting spectra processor")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.MakeSpectraFiles: Spectra generation started")
            End If

			'Loop until the spectra generator finishes
			While (SpectraGen.Status = ISpectraFileProcessor.ProcessStatus.SF_STARTING) Or _
			 (SpectraGen.Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING)
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, 0, SpectraGen.SpectraFileCount, "", "", "", False)
                System.Threading.Thread.Sleep(5000)              'Delay for 5 seconds

			End While
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.MakeSpectraFiles: Exception while generating dta files: " & ex.Message)
            m_message = AppendToComment(m_message, "Exception while generating dta files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Set internal spectra file count to that returned by the spectra generator
		m_DtaCount = SpectraGen.SpectraFileCount

        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, 0, m_DtaCount, "", "", "", False)

		'Check for reason spectra generator exited
        If SpectraGen.Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.MakeSpectraFiles: Error making DTA files: " & SpectraGen.ErrMsg)
            m_message = AppendToComment(m_message, "Error making DTA files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        ElseIf SpectraGen.Results = ISpectraFileProcessor.ProcessResults.SF_ABORTED Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.MakeSpectraFiles: DTA generation aborted")
            m_message = AppendToComment(m_message, "DTA generation aborted")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

		'Return results
		If SpectraGen.Results = ISpectraFileProcessor.ProcessResults.SF_NO_FILES_CREATED Then
			m_message = AppendToComment(m_message, "No spectra files created")
			Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
		Else
			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.MakeSpectraFiles: Spectra generation completed")
            End If
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	''' <summary>
	''' Concatenates DTA files into a single test file
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function ConcatSpectraFiles() As IJobParams.CloseOutType

		'Packages dta files into concatenated text files

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Concatenating spectra files, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))

		Dim ConcatTools As New clsConcatToolWrapper(m_WorkDir)
		If Not ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_DTA, m_jobParams.GetParam("datasetNum")) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error packaging results: " & ConcatTools.ErrMsg & ", job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            m_message = AppendToComment(m_message, "Error packaging results")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

    ''' <summary>
    ''' Deletes .raw files from working directory
    ''' </summary>
    ''' <returns>CloseoutType enum indicating success or failure</returns>
    ''' <remarks>Overridden for other types of input files</remarks>
	Protected Overridable Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .raw file from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.DeleteDataFile, executing method")
        End If

		'Delete the .raw file
		Try
            FoundFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.raw")
			For Each MyFile In FoundFiles
				If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.DeleteDataFile, deleting file " & MyFile)
                End If
				DeleteFileWithRetries(MyFile)
			Next
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting .raw file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	''' <summary>
	''' Zips concatenated DTA file to reduce size
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function ZipConcDtaFile() As IJobParams.CloseOutType

		'Zips the concatenated dta file
		Dim DtaFileName As String = m_jobParams.GetParam("datasetNum") & "_dta.txt"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Zipping concatenated spectra file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))

		'Verify file exists
        If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, DtaFileName)) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to find concatenated dta file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

		'Zip the file
        Try
            Dim ZipProgramPath As String = m_mgrParams.GetParam("zipprogram")
            If ZipProgramPath Is Nothing Then ZipProgramPath = String.Empty
            If Not System.IO.File.Exists(ZipProgramPath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Zip program not found: " & ZipProgramPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim Zipper As New ZipTools(m_WorkDir, ZipProgramPath)
            Dim ZipFileName As String = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(DtaFileName)) & ".zip"
            If Not Zipper.MakeZipFile("-fast", ZipFileName, DtaFileName) Then
                Dim Msg As String = "Error zipping concat dta file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Catch ex As Exception
            Dim Msg As String = "Exception zipping concat dta file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function
#End Region

End Class
