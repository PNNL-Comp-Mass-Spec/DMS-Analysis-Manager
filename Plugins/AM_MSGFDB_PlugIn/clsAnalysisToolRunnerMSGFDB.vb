'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports Microsoft.Hpc.Scheduler

Public Class clsAnalysisToolRunnerMSGFDB
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSGFDB or MSGF+ analysis
	'*********************************************************************************************************

#Region "Module Variables"

	Protected mToolVersionWritten As Boolean

	' Path to MSGFDB.jar or MSGFPlus.jar
	Protected mMSGFDbProgLoc As String

	Protected mMSGFDbProgLocHPC As String

	Protected mMSGFPlus As Boolean

	Protected mResultsIncludeAutoAddedDecoyPeptides As Boolean = False

	Protected mWorkingDirectoryInUse As String

	Protected mUsingHPC As Boolean
	Protected mHPCJobID As Integer

	Protected mMSGFPlusComplete As Boolean
	Protected mMSGFPlusCompletionTime As DateTime

	Protected WithEvents mMSGFDBUtils As clsMSGFDBUtils

	Protected WithEvents CmdRunner As clsRunDosProgram

	Protected WithEvents mComputeCluster As HPC_Submit.WindowsHPC2012

	Protected WithEvents mHPCMonitorInitTimer As Timers.Timer

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSGFDB tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType
		Dim CmdStr As String
		Dim intJavaMemorySize As Integer
		Dim strMSGFDbCmdLineOptions As String = String.Empty

		Dim result As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False
		Dim blnTooManySkippedSpectra As Boolean = False
		Dim blnSuccess As Boolean

		Dim FastaFilePath As String = String.Empty
		Dim FastaFileSizeKB As Single
		Dim FastaFileIsDecoy As Boolean

		Dim blnUsingMzXML As Boolean
		Dim strScanTypeFilePath As String = String.Empty
		Dim strAssumedScanType As String = String.Empty
		Dim ResultsFileName As String

		Dim Msg As String

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSGFDB.RunTool(): Enter")
			End If

			mHPCMonitorInitTimer = New Timers.Timer(30000)
			mHPCMonitorInitTimer.Enabled = False

			' Determine whether or not we'll be running MSGF+ in HPC (high performance computing) mode
			Dim udtHPCOptions As clsAnalysisResources.udtHPCOptionsType = clsAnalysisResources.GetHPCOptions(m_jobParams, m_MachName)

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
			Dim JavaProgLoc = GetJavaProgLoc()
			If String.IsNullOrEmpty(JavaProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim strMSGFJarfile As String
			Dim strSearchEngineName As String

			' Obsolete setting:
			'Dim blnUseLegacyMSGFDB = clsMSGFDBUtils.UseLegacyMSGFDB(m_jobParams)

			'If blnUseLegacyMSGFDB Then
			'	mMSGFPlus = False
			'	strMSGFJarfile = clsMSGFDBUtils.MSGFDB_JAR_NAME
			'	strSearchEngineName = "MS-GFDB"
			'Else
			'	mMSGFPlus = True
			'End If

			mMSGFPlus = True
			strMSGFJarfile = clsMSGFDBUtils.MSGFPLUS_JAR_NAME
			strSearchEngineName = "MSGF+"


			' Determine the path to MSGFDB (or MSGF+)
			' It is important that you pass "MSGFDB" to this function, even if mMSGFPlus = True
			' The reason?  The PeptideHitResultsProcessor (and possibly other software) expects the Tool Version file to be named Tool_Version_Info_MSGFDB.txt
			mMSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", strMSGFJarfile)

			If String.IsNullOrWhiteSpace(mMSGFDbProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If udtHPCOptions.UsingHPC Then
				' Make sure the MSGF+ program is up-to-date on the HPC share
				' Warning: if MSGF+ is running and the .jar file gets updated, then the running jobs will fail because MSGF+ will throw an exception
				' This function will store the path to the MSGF+ jar file in mMSGFDbProgLocHPC
				VerifyHPCMSGFDb(udtHPCOptions)
			End If

			' Note: we will store the MSGFDB version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
			mToolVersionWritten = False

			mUsingHPC = False
			mMSGFPlusComplete = False

			result = DetermineAssumedScanType(strAssumedScanType, blnUsingMzXML, strScanTypeFilePath)
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return result
			End If

			' Initialize mMSGFDBUtils
			mMSGFDBUtils = New clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, mMSGFPlus)

			' Get the FASTA file and index it if necessary
			' Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
			Dim strParameterFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
			Dim javaExePath = String.Copy(JavaProgLoc)
			Dim msgfdbJarFilePath = String.Copy(mMSGFDbProgLoc)

			If udtHPCOptions.UsingHPC Then
				If udtHPCOptions.SharePath.StartsWith("\\winhpcfs") Then
					javaExePath = "\\winhpcfs\projects\DMS\jre8\bin\java.exe"
				Else
					javaExePath = "\\picfs.pnl.gov\projects\DMS\jre8\bin\java.exe"
				End If
				msgfdbJarFilePath = mMSGFDbProgLocHPC
			End If

			result = mMSGFDBUtils.InitializeFastaFile(javaExePath, msgfdbJarFilePath, FastaFileSizeKB, FastaFileIsDecoy, FastaFilePath, strParameterFilePath, udtHPCOptions)

			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return result
			End If

			Dim strInstrumentGroup As String = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

			' Read the MSGFDB Parameter File
			result = mMSGFDBUtils.ParseMSGFDBParameterFile(FastaFileSizeKB, FastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, udtHPCOptions, strMSGFDbCmdLineOptions)
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return result
			ElseIf String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Problem parsing " & strSearchEngineName & " parameter file"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
			mResultsIncludeAutoAddedDecoyPeptides = mMSGFDBUtils.ResultsIncludeAutoAddedDecoyPeptides

			If mMSGFPlus Then
				ResultsFileName = m_Dataset & "_msgfplus.mzid"
			Else
				ResultsFileName = m_Dataset & "_msgfdb.txt"
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running " & strSearchEngineName)

			' If an MSGFDB analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
			' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
			' (job 611216 succeeded with a value of 5000)
			intJavaMemorySize = m_jobParams.GetJobParameter("MSGFDBJavaMemorySize", 2000)
			If intJavaMemorySize < 512 Then intJavaMemorySize = 512

			If udtHPCOptions.UsingHPC AndAlso intJavaMemorySize < 10000 Then
				' Automatically bump up the memory to use to 28 GB  (the machines have 32 GB per socket)
				intJavaMemorySize = 28000
			End If

			'Set up and execute a program runner to run MSGFDB
			CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & msgfdbJarFilePath

			' Define the input file, output file, and fasta file
			If blnUsingMzXML Then
				CmdStr &= " -s " & m_Dataset & ".mzXML"
			Else
				CmdStr &= " -s " & m_Dataset & "_dta.txt"
			End If

			CmdStr &= " -o " & ResultsFileName
			CmdStr &= " -d " & PossiblyQuotePath(FastaFilePath)

			' Append the remaining options loaded from the parameter file
			CmdStr &= " " & strMSGFDbCmdLineOptions

			' Make sure the machine has enough free memory to run MSGFDB
			Dim blnLogFreeMemoryOnSuccess = Not m_DebugLevel < 1

			If Not udtHPCOptions.UsingHPC Then
				If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, strSearchEngineName, blnLogFreeMemoryOnSuccess) Then
					m_message = "Not enough free memory to run " & strSearchEngineName
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

			mWorkingDirectoryInUse = String.Copy(m_WorkDir)

			If udtHPCOptions.UsingHPC Then
				Dim criticalError As Boolean

				blnSuccess = StartMSGFPlusHPC(javaExePath, msgfdbJarFilePath, strSearchEngineName, ResultsFileName, CmdStr, udtHPCOptions, criticalError)

				If criticalError Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			Else
				blnSuccess = StartMSGFPlusLocal(javaExePath, strSearchEngineName, CmdStr)
			End If

			If Not blnSuccess And String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
				' Parse the console output file one more time in hopes of finding an error message
				ParseConsoleOutputFile(mWorkingDirectoryInUse)
			End If

			If Not mToolVersionWritten Then
				If String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
					ParseConsoleOutputFile(mWorkingDirectoryInUse)
				End If
				mToolVersionWritten = StoreToolVersionInfo()
			End If

			If Not String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSGFDBUtils.ConsoleOutputErrorMsg)
			End If

			If blnSuccess Then
				If Not mMSGFPlusComplete Then
					mMSGFPlusComplete = True
					mMSGFPlusCompletionTime = DateTime.UtcNow
				End If
			Else
				If mMSGFPlusComplete Then
					Msg = "MSGF+ log file reported it was complete, but aborted the ProgRunner since Java was frozen"
				Else
					Msg = "Error running " & strSearchEngineName
				End If
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If udtHPCOptions.UsingHPC And mMSGFPlusComplete Then
					' Don't treat this as a fatal error; HPC jobs don't always close out cleanly
					blnProcessingError = False
					m_EvalMessage = String.Copy(m_message)
					m_message = String.Empty
				Else
					blnProcessingError = True
				End If

				If Not udtHPCOptions.UsingHPC And Not mMSGFPlusComplete Then
					If CmdRunner.ExitCode <> 0 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strSearchEngineName & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & strSearchEngineName & " failed (but exit code is 0)")
					End If
				End If

			End If

			If mMSGFPlusComplete Then
				m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE
				m_StatusTools.UpdateAndWrite(m_progress)
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB Search Complete")
				End If

				If mMSGFDBUtils.ContinuumSpectraSkipped > 0 Then
					' See if any spectra were processed
					If Not File.Exists(Path.Combine(m_WorkDir, ResultsFileName)) Then
						' Note that DMS stored procedure AutoResetFailedJobs looks for jobs with these phrases in the job comment
						'   "None of the spectra are centroided; unable to process"
						'   "skipped xx% of the spectra because they did not appear centroided"
						'   "skip xx% of the spectra because they did not appear centroided"
						'
						' Failed jobs that are found to have this comment will have their settings files auto-updated and the job will auto-reset

						m_message = clsAnalysisResources.SPECTRA_ARE_NOT_CENTROIDED & " with " & strSearchEngineName
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						blnProcessingError = True
					Else
						' Compute the fraction of all potential spectra that were skipped
						Dim dblFractionSkipped As Double
						Dim strPercentSkipped As String

						dblFractionSkipped = mMSGFDBUtils.ContinuumSpectraSkipped / (mMSGFDBUtils.ContinuumSpectraSkipped + mMSGFDBUtils.SpectraSearched)
						strPercentSkipped = (dblFractionSkipped * 100).ToString("0.0") & "%"

						If dblFractionSkipped > 0.2 Then
							m_message = strSearchEngineName & " skipped " & strPercentSkipped & " of the spectra because they did not appear centroided"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							blnTooManySkippedSpectra = True
						Else
							m_EvalMessage = strSearchEngineName & " processed some of the spectra, but it skipped " & mMSGFDBUtils.ContinuumSpectraSkipped & " spectra that were not centroided (" & strPercentSkipped & " skipped)"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
						End If

					End If

				End If

			End If

			' Look for the .mzid file
			' If it exists, then call PostProcessMSGFDBResults even if blnProcessingError is true
			Dim fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, ResultsFileName))

			If fiResultsFile.Exists Then
				result = PostProcessMSGFDBResults(ResultsFileName, JavaProgLoc, udtHPCOptions)
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error post-processing the " & strSearchEngineName & " results"
					End If
					blnProcessingError = True
				End If
			Else
				If String.IsNullOrEmpty(m_message) Then
					m_message = "MSGF+ results file not found: " & ResultsFileName
					blnProcessingError = True
				End If
			End If

			' Copy any newly created files from PIC back to the local working directory
			' ToDo: Uncomment this code if we run the PeptideToProteinMapper on HPC
			' SynchronizeFolders(udtHPCOptions.WorkDirPath, m_WorkDir)

			m_progress = clsMSGFDBUtils.PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			Threading.Thread.Sleep(2000)		   '2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If udtHPCOptions.UsingHPC Then
				' Delete files in the working directory on PIC
				m_FileTools.DeleteDirectoryFiles(udtHPCOptions.WorkDirPath, False)
			End If

			If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Something went wrong
				' In order to help diagnose things, we will move whatever files were created into the result folder, 
				'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
				CopyFailedResultsToArchiveFolder()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MakeResultsFolder()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'MakeResultsFolder handles posting to local log, so set database error message and exit
				m_message = "Error making results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MoveResultFiles()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				m_message = "Error moving files into results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = CopyResultsFolderToServer()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				Return result
			End If

		Catch ex As Exception
			m_message = "Error in MSGFDbPlugin->RunTool: " & ex.Message
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		If blnTooManySkippedSpectra Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If


	End Function

	'Public Shared Function GetHPCReleaseDelayTask() As HPC_Connector.ParametersTask

	'	Dim oNodeReleaseDelay As New HPC_Connector.ParametersTask("Wait")
	'	oNodeReleaseDelay.TaskTypeOption = HPC_Connector.HPCTaskType.NodeRelease
	'	Const waitTimeSeconds As Integer = 35
	'	oNodeReleaseDelay.CommandLine = "ping 1.1.1.1 -n 1 -w " & waitTimeSeconds * 1000 & " > nul"

	'	Return oNodeReleaseDelay

	'End Function

	Public Shared Function MakeHPCBatchFile(ByVal workDirPath As String, ByVal batchFileName As String, ByVal commandToRun As String) As String

		Const waitTimeSeconds As Integer = 35

		Dim batchFilePath = Path.Combine(workDirPath, batchFileName)

		Using swBatchFile = New StreamWriter(New FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
			swBatchFile.WriteLine("@echo off")
			swBatchFile.WriteLine(commandToRun)
			'swBatchFile.WriteLine("ping 1.1.1.1 -n 1 -w " & waitTimeSeconds * 1000 & " > nul")
			swBatchFile.WriteLine("\\picfs.pnl.gov\projects\DMS\DMS_Programs\Utilities\sleep " & waitTimeSeconds)
			swBatchFile.WriteLine("echo Success")
		End Using

		Return batchFilePath

	End Function

	Protected Function StartMSGFPlusHPC(
	  ByVal javaExePath As String,
	  ByVal msgfdbJarFilePath As String,
	  ByVal strSearchEngineName As String,
	  ByVal ResultsFileName As String,
	  ByVal CmdStr As String,
	  ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType,
	  ByRef criticalError As Boolean) As Boolean

		mWorkingDirectoryInUse = String.Copy(udtHPCOptions.WorkDirPath)
		mUsingHPC = True
		criticalError = False

		' Synchronize local files to the remote working directory on PIC

		Dim lstFileNameFilterSpec = New List(Of String)
		Dim lstFileNameExclusionSpec = New List(Of String) From {m_Dataset & "_ScanStats.txt", m_Dataset & "_ScanStatsEx.txt", "Mass_Correction_Tags.txt"}

		SynchronizeFolders(m_WorkDir, mWorkingDirectoryInUse, lstFileNameFilterSpec, lstFileNameExclusionSpec)

		Dim jobStep = m_jobParams.GetJobParameter("StepParameters", "Step", 1)

		Dim jobName As String = strSearchEngineName & "_Job" & m_JobNum & "_Step" & jobStep

		Dim hpcJobInfo = New HPC_Connector.JobToHPC(udtHPCOptions.HeadNode, jobName, taskName:=strSearchEngineName)

		hpcJobInfo.JobParameters.PriorityLevel = HPC_Connector.PriorityLevel.Normal
		hpcJobInfo.JobParameters.TemplateName = "DMS"		 ' If using 32 cores, could use Template "Single"
		hpcJobInfo.JobParameters.ProjectName = "DMS"

		' April 2014 note: If using picfs.pnl.gov  then we must reserve an entire node due to file system issues of the Windows Nodes talking to the Isilon file system
		' Each node has two sockets

		'hpcJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Socket
		hpcJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Node
		hpcJobInfo.JobParameters.isExclusive = True

		' If requesting a socket or a node, there is no need to set the number of cores
		' hpcJobInfo.JobParameters.MinNumberOfCores = udtHPCOptions.MinimumCores
		' hpcJobInfo.JobParameters.MaxNumberOfCores = udtHPCOptions.MinimumCores

		If udtHPCOptions.SharePath.StartsWith("\\picfs") Then
			' Make a batch file that will run the java program, then sleep for 35 seconds, which should allow the file system to release the file handles
			Dim batchFilePath = MakeHPCBatchFile(udtHPCOptions.WorkDirPath, "HPC_MSGFPlus_Task.bat", javaExePath & " " & CmdStr)
			m_jobParams.AddResultFileToSkip(batchFilePath)

			hpcJobInfo.TaskParameters.CommandLine = batchFilePath
		Else
			' Simply run java; no need to add a delay
			hpcJobInfo.TaskParameters.CommandLine = javaExePath & " " & CmdStr
		End If
		
		hpcJobInfo.TaskParameters.WorkDirectory = udtHPCOptions.WorkDirPath
		hpcJobInfo.TaskParameters.StdOutFilePath = Path.Combine(udtHPCOptions.WorkDirPath, clsMSGFDBUtils.MSGFDB_CONSOLE_OUTPUT_FILE)
		hpcJobInfo.TaskParameters.TaskTypeOption = HPC_Connector.HPCTaskType.Basic
		hpcJobInfo.TaskParameters.FailJobOnFailure = True

		' Set the maximum runtime to 3 days
		' Note that this runtime includes the time the job is queued, plus also the time the job is running
		hpcJobInfo.JobParameters.MaxRunTimeHours = 72

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, hpcJobInfo.TaskParameters.CommandLine)
		End If

		If mMSGFPlus Then
			Dim mzidToTSVTask = New HPC_Connector.ParametersTask("MZID_To_TSV")
			Dim tsvFileName = m_Dataset & clsMSGFDBUtils.MSGFDB_TSV_SUFFIX
			Const tsvConversionJavaMemorySizeMB As Integer = 4000

			Dim cmdStrConvertToTSV = clsMSGFDBUtils.GetMZIDtoTSVCommandLine(ResultsFileName, tsvFileName, udtHPCOptions.WorkDirPath, msgfdbJarFilePath, tsvConversionJavaMemorySizeMB)

			If udtHPCOptions.SharePath.StartsWith("\\picfs") Then
				' Make a batch file that will run the java program, then sleep for 35 seconds, which should allow the file system to release the file handles
				Dim tsvBatchFilePath = MakeHPCBatchFile(udtHPCOptions.WorkDirPath, "HPC_TSV_Task.bat", javaExePath & " " & cmdStrConvertToTSV)
				m_jobParams.AddResultFileToSkip(tsvBatchFilePath)

				mzidToTSVTask.CommandLine = tsvBatchFilePath
			Else
				' Simply run java; no need to add a delay
				mzidToTSVTask.CommandLine = javaExePath & " " & cmdStrConvertToTSV
			End If

			mzidToTSVTask.WorkDirectory = udtHPCOptions.WorkDirPath
			mzidToTSVTask.StdOutFilePath = Path.Combine(udtHPCOptions.WorkDirPath, "MzIDToTsv_ConsoleOutput.txt")
			mzidToTSVTask.TaskTypeOption = HPC_Connector.HPCTaskType.Basic
			mzidToTSVTask.FailJobOnFailure = True

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mzidToTSVTask.CommandLine)
			End If

			hpcJobInfo.SubsequentTaskParameters.Add(mzidToTSVTask)
		End If

		Dim sPICHPCUsername = m_mgrParams.GetParam("PICHPCUser", "")
		Dim sPICHPCPassword = m_mgrParams.GetParam("PICHPCPassword", "")

		If String.IsNullOrEmpty(sPICHPCUsername) Then
			m_message = "Manager parameter PICHPCUser is undefined; unable to schedule HPC job"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			criticalError = True
			Return False
		End If

		If String.IsNullOrEmpty(sPICHPCPassword) Then
			m_message = "Manager parameter PICHPCPassword is undefined; unable to schedule HPC job"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			criticalError = True
			Return False
		End If

		mComputeCluster = New HPC_Submit.WindowsHPC2012(sPICHPCUsername, clsGlobal.DecodePassword(sPICHPCPassword))
		mHPCJobID = mComputeCluster.Send(hpcJobInfo)

		Dim blnSuccess As Boolean

		If mHPCJobID <= 0 Then
			m_message = strSearchEngineName & " Job was not created in HPC: " & mComputeCluster.ErrorMessage
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			blnSuccess = False
		Else
			blnSuccess = True
		End If

		If blnSuccess Then
			If mComputeCluster.Scheduler Is Nothing Then
				m_message = "Error: HPC Scheduler is null for " & strSearchEngineName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				blnSuccess = False
			End If
		End If

		If blnSuccess Then
			Dim hpcJob = mComputeCluster.Scheduler.OpenJob(mHPCJobID)

			mHPCMonitorInitTimer.Enabled = True

			blnSuccess = mComputeCluster.MonitorJob(hpcJob)

			If Not blnSuccess Then

				'ExamineHPCTasks(hpcJob)

				m_message = "HPC Job Monitor returned false"
				If Not String.IsNullOrWhiteSpace(mComputeCluster.ErrorMessage) Then
					m_message &= ": " & mComputeCluster.ErrorMessage
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			End If

			' Copy any newly created files from PIC back to the local working directory
			SynchronizeFolders(udtHPCOptions.WorkDirPath, m_WorkDir)

			' Rename the Tool_Version_Info file
			Dim fiToolVersionInfo = New FileInfo(Path.Combine(m_WorkDir, "Tool_Version_Info_MSGFDB_HPC.txt"))
			If Not fiToolVersionInfo.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ToolVersionInfo file not found; this will lead to problems with IDPicker: " & fiToolVersionInfo.FullName)
			Else
				fiToolVersionInfo.MoveTo(Path.Combine(m_WorkDir, "Tool_Version_Info_MSGFDB.txt"))
			End If
		End If

		Return blnSuccess

	End Function

	Private Sub ExamineHPCTasks(ByVal hpcJob As ISchedulerJob)

		Try
			Dim properties As Microsoft.Hpc.Scheduler.IPropertyIdCollection = Nothing
			Dim filter As Microsoft.Hpc.Scheduler.IFilterCollection = Nothing
			Dim sort As Microsoft.Hpc.Scheduler.ISortCollection = Nothing
			Dim expandParametric As Boolean

			Dim taskIterator = hpcJob.OpenTaskEnumerator(properties, filter, sort, expandParametric)
			Dim oTaskEnum = taskIterator.GetEnumerator()

			' Step through the tasks to examine them
			While oTaskEnum.MoveNext
				Dim oPropertyEnum = oTaskEnum.Current.GetEnumerator()
				While oPropertyEnum.MoveNext
					Console.WriteLine(oPropertyEnum.Current.PropName.ToString() & ": " & oPropertyEnum.Current.Value.ToString())
				End While
			End While

		Catch ex As Exception
			Console.WriteLine("Error examining job tasks: " & ex.Message)
		End Try
	
		
	End Sub

	Protected Function StartMSGFPlusLocal(ByVal javaExePath As String, ByVal strSearchEngineName As String, ByVal CmdStr As String) As Boolean

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaExePath & " " & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = True
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, clsMSGFDBUtils.MSGFDB_CONSOLE_OUTPUT_FILE)
		End With

		m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_STARTING

		Dim blnSuccess = CmdRunner.RunProgram(javaExePath, CmdStr, strSearchEngineName, True)

		Return blnSuccess

	End Function

	''' <summary>
	''' Convert the .mzid file created by MSGF+ to a .tsv file
	''' </summary>
	''' <param name="strMZIDFileName"></param>
	''' <param name="JavaProgLoc"></param>
	''' <returns>The name of the .tsv file if successful; empty string if an error</returns>
	''' <remarks></remarks>
	Protected Function ConvertMZIDToTSV(ByVal strMZIDFileName As String, ByVal JavaProgLoc As String, udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As String

		Dim blnConversionRequired = True

		Dim strTSVFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsMSGFDBUtils.MSGFDB_TSV_SUFFIX)

		If udtHPCOptions.UsingHPC Then
			' The TSV file should have already been created by the HPC job, then copied locally via SynchronizeFolders

			Dim fiTSVFile = New FileInfo(strTSVFilePath)
			If fiTSVFile.Exists Then
				blnConversionRequired = False
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF+ TSV file was not created by HPC; missing " & fiTSVFile.Name)
			End If
		End If

		If blnConversionRequired Then
			strTSVFilePath = mMSGFDBUtils.ConvertMZIDToTSV(JavaProgLoc, mMSGFDbProgLoc, m_Dataset, strMZIDFileName)

			If String.IsNullOrEmpty(strTSVFilePath) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error calling mMSGFDBUtils.ConvertMZIDToTSV; path not returned"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End If
				Return String.Empty
			End If
		End If

		Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)

		If splitFastaEnabled Then
			Dim tsvFileName = ParallelMSGFPlusRenameFile(Path.GetFileName(strTSVFilePath))
			Return tsvFileName
		Else
			Return Path.GetFileName(strTSVFilePath)
		End If

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		mMSGFDBUtils.DeleteFileInWorkDir(m_Dataset & "_dta.txt")
		mMSGFDBUtils.DeleteFileInWorkDir(m_Dataset & "_dta.zip")

		' Make the results folder
		result = MakeResultsFolder()
		If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Move the result files into the result folder
			result = MoveResultFiles()
			If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Move was a success; update strFolderPathToArchive
				strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	Protected Function CreateScanTypeFile(ByRef strScanTypeFilePath As String) As Boolean

		Dim objScanTypeFileCreator As clsScanTypeFileCreator
		objScanTypeFileCreator = New clsScanTypeFileCreator(m_WorkDir, m_Dataset)

		strScanTypeFilePath = String.Empty

		If objScanTypeFileCreator.CreateScanTypeFile() Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Created ScanType file: " & Path.GetFileName(objScanTypeFileCreator.ScanTypeFilePath))
			End If
			strScanTypeFilePath = objScanTypeFileCreator.ScanTypeFilePath
			Return True
		Else
			Dim strErrorMessage = "Error creating scan type file: " & objScanTypeFileCreator.ErrorMessage
			m_message = String.Copy(strErrorMessage)

			If Not String.IsNullOrEmpty(objScanTypeFileCreator.ExceptionDetails) Then
				strErrorMessage &= "; " & objScanTypeFileCreator.ExceptionDetails
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage)
			Return False
		End If

	End Function

	Protected Function DetermineAssumedScanType(ByRef strAssumedScanType As String, ByRef blnUsingMzXML As Boolean, ByRef strScanTypeFilePath As String) As IJobParams.CloseOutType
		Dim strScriptNameLCase As String
		strAssumedScanType = String.Empty

		strScriptNameLCase = m_jobParams.GetParam("ToolName").ToLower()
		strScanTypeFilePath = String.Empty

		If strScriptNameLCase.Contains("mzxml") OrElse strScriptNameLCase.Contains("msgfplus_bruker") Then
			blnUsingMzXML = True
		Else
			blnUsingMzXML = False

			' Make sure the _DTA.txt file is valid
			If Not ValidateCDTAFile() Then
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			End If

			strAssumedScanType = m_jobParams.GetParam("AssumedScanType")

			If String.IsNullOrWhiteSpace(strAssumedScanType) Then
				' Create the ScanType file (lists scan type for each scan number)
				If Not CreateScanTypeFile(strScanTypeFilePath) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Sub MonitorMSGFPlusProgress()

		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
		Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 15 seconds)
		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 15 Then
			dtLastStatusUpdate = DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 30 Then
			dtLastConsoleOutputParse = DateTime.UtcNow

			ParseConsoleOutputFile(mWorkingDirectoryInUse)
			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

			If m_progress >= clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE Then
				If Not mMSGFPlusComplete Then
					mMSGFPlusComplete = True
					mMSGFPlusCompletionTime = DateTime.UtcNow
				Else
					If DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes >= 5 Then
						' MSGF+ is stuck at 96% complete and has been that way for 5 minutes
						' Java is likely frozen and thus the process should be aborted
						Dim warningMessage = "MSGF+ has been stuck at " & clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE.ToString("0") & "% complete for 5 minutes; aborting since Java appears frozen"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)

						' Bump up mMSGFPlusCompletionTime by one hour
						' This will prevent this function from logging the above message every 30 seconds if the .abort command fails
						mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1)

						If mUsingHPC Then
							mComputeCluster.AbortNow()
						Else
							CmdRunner.AbortProgramNow()
						End If

					End If
				End If
			End If

		End If
	End Sub

	''' <summary>
	''' Renames the results file created by a Parallel MSGF+ instance to have _Part##.mzid as a suffix
	''' </summary>
	''' <param name="resultsFileName"></param>
	''' <returns>The path to the new file if success, otherwise the original filename</returns>
	''' <remarks></remarks>
	Private Function ParallelMSGFPlusRenameFile(ByVal resultsFileName As String) As String

		Dim filePathNew = "??"

		Try
			Dim fiFile = New FileInfo(Path.Combine(m_WorkDir, resultsFileName))

			Dim iteration = clsAnalysisResources.GetSplitFastaIteration(m_jobParams, m_message)

			Dim fileNameNew = Path.GetFileNameWithoutExtension(fiFile.Name) & "_Part" & iteration & fiFile.Extension

			If Not fiFile.Exists Then Return resultsFileName

			filePathNew = Path.Combine(m_WorkDir, fileNameNew)
			fiFile.MoveTo(filePathNew)

			Return fileNameNew

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error renaming file " & resultsFileName & " to " & filePathNew, ex)
			Return (resultsFileName)
		End Try

	End Function

	''' <summary>
	''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
	''' </summary>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal workingDirectory As String)

		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow
		Dim sngMSGFBProgress As Single = 0

		Try
			If Not mMSGFDBUtils Is Nothing Then
				sngMSGFBProgress = mMSGFDBUtils.ParseMSGFDBConsoleOutputFile(workingDirectory)
			End If

			If m_progress < sngMSGFBProgress Then
				m_progress = sngMSGFBProgress

				If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
					dtLastProgressWriteTime = DateTime.UtcNow
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
				End If
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file: " & ex.Message)
			End If
		End Try

	End Sub

	''' <summary>
	''' Convert the .mzid file to a TSV file and create the PeptideToProtein map file
	''' </summary>
	''' <param name="ResultsFileName"></param>
	''' <param name="JavaProgLoc"></param>
	''' <param name="udtHPCOptions"></param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>Assumes that the calling function has verified that ResultsFileName exists</remarks>
	Protected Function PostProcessMSGFDBResults(
	  ByVal ResultsFileName As String,
	  ByVal JavaProgLoc As String,
	  ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType
		Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)

		If splitFastaEnabled Then
			ResultsFileName = ParallelMSGFPlusRenameFile(ResultsFileName)
			ParallelMSGFPlusRenameFile("MSGFDB_ConsoleOutput.txt")
		End If

		' Zip the output file
		result = mMSGFDBUtils.ZipOutputFile(Me, ResultsFileName)
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		If Not mMSGFPlus Then
			m_jobParams.AddResultFileToSkip(ResultsFileName & ".temp.tsv")
		End If

		Dim strMSGFDBResultsFileName As String
		If Path.GetExtension(ResultsFileName).ToLower() = ".mzid" Then

			' Convert the .mzid file to a .tsv file 
			' If running on HPC this should have already happened, but we need to call ConvertMZIDToTSV() anyway to possibly rename the .tsv file

			UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_CONVERT_MZID_TO_TSV)
			strMSGFDBResultsFileName = ConvertMZIDToTSV(ResultsFileName, JavaProgLoc, udtHPCOptions)

			If String.IsNullOrEmpty(strMSGFDBResultsFileName) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Else
			strMSGFDBResultsFileName = String.Copy(ResultsFileName)
		End If

		' Create the Peptide to Protein map file
		' ToDo: If udtHPCOptions.UsingPIC = True, then run this on PIC by calling 64-bit PeptideToProteinMapper.exe

		UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS)

		Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

		If udtHPCOptions.UsingHPC Then
			' Override the OrgDbDir to point to Picfs
			localOrgDbFolder = Path.Combine(udtHPCOptions.SharePath, "DMS_Temp_Org")
		End If

		result = mMSGFDBUtils.CreatePeptideToProteinMapping(strMSGFDBResultsFileName, mResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder)
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
			Return result
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mMSGFDBUtils.MSGFDbVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(mMSGFDbProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
		m_progress = sngPercentComplete
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
	End Sub

	Protected Function VerifyHPCMSGFDb(udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

		Try
			' Make sure the copy of MSGF+ is up-to-date on PICfs
			Dim fiMSGFDbProg = New FileInfo(mMSGFDbProgLoc)
			Dim strMSGFDbRelativePath = fiMSGFDbProg.Directory.FullName
			Dim chDMSProgramsIndex = strMSGFDbRelativePath.ToLower().IndexOf("\dms_programs\", System.StringComparison.Ordinal)

			If chDMSProgramsIndex < 0 Then
				m_message = "Unable to determine the relative path to the MSGF+ program folder; could not find \dms_programs\ in " & strMSGFDbRelativePath
				Return False
			End If

			strMSGFDbRelativePath = strMSGFDbRelativePath.Substring(chDMSProgramsIndex + 1)

			Dim strTargetDirectory = Path.Combine(udtHPCOptions.SharePath, strMSGFDbRelativePath)
			mMSGFDbProgLocHPC = Path.Combine(strTargetDirectory, fiMSGFDbProg.Name)

			Dim success = SynchronizeFolders(fiMSGFDbProg.Directory.FullName, strTargetDirectory, fiMSGFDbProg.Name)

			If Not success Then
				If String.IsNullOrWhiteSpace(m_message) Then
					m_message = "SynchronizeFolders returned false validating " & fiMSGFDbProg.Name & " on HPC"
				End If
				Return False
			End If

		Catch ex As Exception
			m_message = "Error in VerifyHPCMSGFDb"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		MonitorMSGFPlusProgress()
	End Sub

	Private Sub mMSGFDBUtils_ErrorEvent(ByVal errorMessage As String, ByVal detailedMessage As String) Handles mMSGFDBUtils.ErrorEvent
		m_message = String.Copy(errorMessage)
		If String.IsNullOrEmpty(detailedMessage) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage)
		End If

	End Sub

	Private Sub mMSGFDBUtils_IgnorePreviousErrorEvent() Handles mMSGFDBUtils.IgnorePreviousErrorEvent
		m_message = String.Empty
	End Sub

	Private Sub mMSGFDBUtils_MessageEvent(messageText As String) Handles mMSGFDBUtils.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, messageText)
	End Sub

	Private Sub mMSGFDBUtils_WarningEvent(warningMessage As String) Handles mMSGFDBUtils.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
	End Sub

	Private Sub mComputeCluster_ErrorEvent(sender As Object, e As HPC_Submit.MessageEventArgs) Handles mComputeCluster.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, e.Message)
	End Sub

	Private Sub mComputeCluster_MessageEvent(sender As Object, e As HPC_Submit.MessageEventArgs) Handles mComputeCluster.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message)
	End Sub

	Private Sub mComputeCluster_ProgressEvent(sender As Object, e As HPC_Submit.ProgressEventArgs) Handles mComputeCluster.ProgressEvent
		mHPCMonitorInitTimer.Enabled = False
		MonitorMSGFPlusProgress()
	End Sub

	''' <summary>
	''' This timer is started just before the call to mComputeCluster.MonitorJob
	''' The event will fire very 30 seconds, allowing the manager to update its status
	''' </summary>
	''' <param name="sender"></param>
	''' <param name="e"></param>
	''' <remarks>When event mComputeCluster.ProgressEvent fires, it will disable this timer</remarks>
	Private Sub mHPCMonitorInitTimer_Elapsed(sender As Object, e As System.Timers.ElapsedEventArgs) Handles mHPCMonitorInitTimer.Elapsed
		UpdateStatusRunning(m_progress)
	End Sub

#End Region

End Class
