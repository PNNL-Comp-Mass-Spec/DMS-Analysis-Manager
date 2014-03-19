'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/29/2008
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports MsMsDataFileReader
Imports System.Collections.Generic
Imports System.IO

Public Class clsDtaGenToolRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Base class for DTA generation tool runners
	'*********************************************************************************************************

#Region "Constants and Enums"
	Public Const CDTA_FILE_SUFFIX As String = "_dta.txt"
	Protected Const CENTROID_CDTA_PROGRESS_START As Integer = 70

	Public Enum eDTAGeneratorConstants
		Unknown = 0
		ExtractMSn = 1
		DeconMSn = 2
		MSConvert = 3
		MGFtoDTA = 4
		DeconConsole = 5
	End Enum
#End Region

#Region "Module-wide variables"
	Protected m_CentroidDTAs As Boolean
	Protected m_ConcatenateDTAs As Boolean
	Protected m_StepNum As Integer
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

		m_StepNum = m_jobParams.GetJobParameter("Step", 0)

		'Create spectra files
		result = CreateMSMSSpectra()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Something went wrong
			' In order to help diagnose things, we will move key files into the result folder, 
			'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
			CopyFailedResultsToArchiveFolder()
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Stop the job timer
		m_StopTime = DateTime.UtcNow

		'Add the current job data to the summary file
		Try
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_StepNum)
			End If
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_StepNum)
		End Try

		'Get rid of raw data file
		result = DeleteDataFile()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		'Add all the extensions of the files to delete after run
		m_jobParams.AddResultFileExtensionToSkip(CDTA_FILE_SUFFIX) ' Unzipped, concatenated DTA
		m_jobParams.AddResultFileExtensionToSkip(".dta")	 ' DTA files
		m_jobParams.AddResultFileExtensionToSkip("DeconMSn_progress.txt")

		'Add any files that are an exception to the captured files to delete list
		m_jobParams.AddResultFileToKeep("lcq_dta.txt")

		result = MakeResultsFolder()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		result = MoveResultFiles()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
			Return result
		End If

		result = CopyResultsFolderToServer()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
			Return result
		End If

		'Make results folder and transfer results
		'result = DispositionResults()
		'If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return result

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'No failures so everything must have succeeded

	End Function

	''' <summary>
	''' Creates DTA files and filters if necessary
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function CreateMSMSSpectra() As IJobParams.CloseOutType

		Dim Result As IJobParams.CloseOutType

		'Make the spectra files
		Result = MakeSpectraFiles()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

		'Concatenate spectra files
		If m_ConcatenateDTAs Then
			Result = ConcatSpectraFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result
		End If

		If m_CentroidDTAs Then
			Result = CentroidCDTA()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result
		End If

		'Zip concatenated spectra files
		Result = ZipConcDtaFile()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

		'If we got to here, everything's OK
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function GetDTAGenerator(ByRef SpectraGen As clsDtaGen) As eDTAGeneratorConstants

		Dim eDtaGeneratorType As eDTAGeneratorConstants
		Dim strErrorMessage As String = String.Empty

		eDtaGeneratorType = GetDTAGeneratorInfo(m_jobParams, m_ConcatenateDTAs, strErrorMessage)

		Select Case eDtaGeneratorType
			Case eDTAGeneratorConstants.MGFtoDTA
				SpectraGen = New clsMGFtoDtaGenMainProcess()

			Case eDTAGeneratorConstants.MSConvert
				SpectraGen = New clsDtaGenMSConvert()

			Case eDTAGeneratorConstants.DeconConsole
				SpectraGen = New clsDtaGenDeconConsole()

			Case eDTAGeneratorConstants.ExtractMSn, eDTAGeneratorConstants.DeconMSn
				SpectraGen = New clsDtaGenThermoRaw()

			Case eDTAGeneratorConstants.Unknown
				If String.IsNullOrEmpty(strErrorMessage) Then
					m_message = "GetDTAGeneratorInfo reported an Unknown DTAGenerator type"
				Else
					m_message = strErrorMessage
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

		End Select

		Return eDtaGeneratorType

	End Function

	Public Shared Function GetDTAGeneratorInfo(ByVal oJobParams As IJobParams, ByRef strErrorMessage As String) As eDTAGeneratorConstants
		Dim blnConcatenateDTAs As Boolean
		Return GetDTAGeneratorInfo(oJobParams, blnConcatenateDTAs, strErrorMessage)
	End Function

	Public Shared Function GetDTAGeneratorInfo(ByVal oJobParams As IJobParams, ByRef blnConcatenateDTAs As Boolean, ByRef strErrorMessage As String) As eDTAGeneratorConstants

		Dim strDTAGenerator As String = oJobParams.GetJobParameter("DtaGenerator", "")
		Dim strRawDataType As String = oJobParams.GetJobParameter("RawDataType", "")
		Dim blnMGFInstrumentData As Boolean = oJobParams.GetJobParameter("MGFInstrumentData", False)

		strErrorMessage = String.Empty
		blnConcatenateDTAs = True

		Dim eRawDataType As clsAnalysisResources.eRawDataTypeConstants

		If String.IsNullOrEmpty(strRawDataType) Then
			strErrorMessage = NotifyMissingParameter(oJobParams, "RawDataType")
			Return eDTAGeneratorConstants.Unknown
		Else
			eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType)
		End If

		If blnMGFInstrumentData Then
			blnConcatenateDTAs = False
			Return eDTAGeneratorConstants.MGFtoDTA
		End If

		Select Case eRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				If strDTAGenerator.ToLower() = clsDtaGenThermoRaw.MSCONVERT_FILENAME.ToLower() Then
					blnConcatenateDTAs = False
					Return eDTAGeneratorConstants.MSConvert

				ElseIf strDTAGenerator.ToLower() = clsDtaGenThermoRaw.DECON_CONSOLE_FILENAME.ToLower() Then
					blnConcatenateDTAs = False
					Return eDTAGeneratorConstants.DeconConsole

				Else
					Select Case strDTAGenerator.ToLower()
						Case clsDtaGenThermoRaw.EXTRACT_MSN_FILENAME.ToLower()
							blnConcatenateDTAs = True
							Return eDTAGeneratorConstants.ExtractMSn

						Case clsDtaGenThermoRaw.DECONMSN_FILENAME.ToLower()
							blnConcatenateDTAs = False
							Return eDTAGeneratorConstants.DeconMSn

						Case Else
							If String.IsNullOrEmpty(strDTAGenerator) Then
								strErrorMessage = NotifyMissingParameter(oJobParams, "DtaGenerator")
							Else
								strErrorMessage = "Unknown DTAGenerator for Thermo Raw files: " & strDTAGenerator
							End If

							Return eDTAGeneratorConstants.Unknown
					End Select

				End If

			Case clsAnalysisResources.eRawDataTypeConstants.mzML
				If strDTAGenerator.ToLower() = clsDtaGenThermoRaw.MSCONVERT_FILENAME.ToLower() Then
					blnConcatenateDTAs = False
					Return eDTAGeneratorConstants.MSConvert

				Else
					strErrorMessage = "Invalid DTAGenerator for mzML files: " & strDTAGenerator
					Return eDTAGeneratorConstants.Unknown
				End If

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
				blnConcatenateDTAs = True
				Return eDTAGeneratorConstants.MGFtoDTA

			Case Else
				strErrorMessage = "Unsupported data type for DTA generation: " & strRawDataType
				Return eDTAGeneratorConstants.Unknown

		End Select

	End Function
	''' <summary>
	''' Detailed method for running a tool
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function DispositionResults() As IJobParams.CloseOutType

		Dim StepResult As IJobParams.CloseOutType

		'Make sure all files have released locks
		PRISM.Processes.clsProgRunner.GarbageCollectNow()
		Threading.Thread.Sleep(1000)

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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_StepNum)
			End If
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_StepNum)
		End Try

		'Delete .dta files
		Dim TmpFile As String
		Dim FileList() As String
		Try
			FileList = Directory.GetFiles(m_WorkDir, "*.dta")
			For Each TmpFile In FileList
				DeleteFileWithRetries(TmpFile)
			Next
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting .dta files, job " & m_JobNum & ", step " & m_StepNum & "; " & Err.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error deleting .dta files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Delete unzipped concatenated dta files
		FileList = Directory.GetFiles(m_WorkDir, "*" & CDTA_FILE_SUFFIX)
		For Each TmpFile In FileList
			Try
				If Path.GetFileName(TmpFile.ToLower) <> "lcq_dta.txt" Then
					DeleteFileWithRetries(TmpFile)
				End If
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error: " & ex.Message & " deleting concatenated dta file, job " & m_JobNum & ", step " & m_StepNum & "; " & ex.Message)
				m_message = clsGlobal.AppendToComment(m_message, "Error packaging results")
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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.DispositionResults(), Exception making results folder, " & Err.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Copy results folder to storage server
		Try
			StepResult = CopyResultsFolderToServer()
			If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return StepResult
			End If
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.DispositionResults(), Exception moving results folder, " & Err.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Protected Function GetDtaGenInitParams() As ISpectraFileProcessor.InitializationParams
		Dim InitParams As ISpectraFileProcessor.InitializationParams
		With InitParams
			.DebugLevel = m_DebugLevel
			.JobParams = m_jobParams
			.MgrParams = m_mgrParams
			.StatusTools = m_StatusTools
			.WorkDir = m_WorkDir
			.DatasetName = m_Dataset
		End With

		Return InitParams
	End Function

	''' <summary>
	''' Creates DTA files
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function MakeSpectraFiles() As IJobParams.CloseOutType

		'Make individual spectra files from input raw data file, using plugin

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Making spectra files, job " & m_JobNum & ", step " & m_StepNum)

		Dim SpectraGen As clsDtaGen = Nothing
		Dim eDTAGenerator As eDTAGeneratorConstants

		eDTAGenerator = GetDTAGenerator(SpectraGen)

		If eDTAGenerator = eDTAGeneratorConstants.Unknown Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If eDTAGenerator = eDTAGeneratorConstants.DeconConsole Then
			m_CentroidDTAs = False
		Else
			m_CentroidDTAs = m_jobParams.GetJobParameter("CentroidDTAs", False)
		End If


		' Initialize the plugin

		Try
			SpectraGen.Setup(GetDtaGenInitParams())
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception configuring DTAGenerator: " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Store the Version info in the database
		Dim blnSuccess As Boolean
		If eDTAGenerator = eDTAGeneratorConstants.MGFtoDTA Then
			' MGFtoDTA Dll
			blnSuccess = StoreToolVersionInfoDLL(SpectraGen.DtaToolNameLoc)
		Else
			If eDTAGenerator = eDTAGeneratorConstants.DeconConsole Then

				' Possibly use a specific version of DeconTools
				Dim progLoc As String
				progLoc = DetermineProgramLocation("DeconMSn", "DeconToolsProgLoc", "DeconConsole.exe")

				If String.IsNullOrWhiteSpace(progLoc) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				Else
					SpectraGen.UpdateDtaToolNameLoc(progLoc)
				End If

			End If

			blnSuccess = StoreToolVersionInfo(SpectraGen.DtaToolNameLoc, eDTAGenerator)
		End If

		If Not blnSuccess Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			m_message = "Error determining " & SpectraGen.DtaToolNameLoc & " version"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Try

			' Start the spectra generation process
			Dim eResult As IJobParams.CloseOutType
			eResult = StartAndWaitForDTAGenerator(SpectraGen, "MakeSpectraFiles", False)

			' Set internal spectra file count to that returned by the spectra generator
			m_DtaCount = SpectraGen.SpectraFileCount
			m_progress = SpectraGen.Progress

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return eResult

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner.MakeSpectraFiles: Exception while generating dta files: " & ex.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Exception while generating dta files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Creates a centroided .mgf file for the dataset
	''' Then updates the _DTA.txt file with the spectral data from the .mgf file
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function CentroidCDTA() As IJobParams.CloseOutType

		Dim strCDTAFileOriginal As String
		Dim strCDTAFileCentroided As String
		Dim strCDTAFileFinal As String

		Try

			' Rename the _DTA.txt file to _DTA_Original.txt
			Dim fiCDTA As FileInfo = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & CDTA_FILE_SUFFIX))
			If Not fiCDTA.Exists() Then
				m_message = "File not found in CentroidCDTA: " & fiCDTA.Name
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			End If

			PRISM.Processes.clsProgRunner.GarbageCollectNow()
			Threading.Thread.Sleep(250)

			strCDTAFileOriginal = Path.Combine(m_WorkDir, m_Dataset & "_DTA_Original.txt")
			fiCDTA.MoveTo(strCDTAFileOriginal)

			m_jobParams.AddResultFileToSkip(fiCDTA.Name)

		Catch ex As Exception
			m_message = "Error renaming the original _DTA.txt file in CentroidCDTA"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try


		Try

			' Create a centroided _DTA.txt file from the .Raw file (first creates a .MGF file, then converts to _DTA.txt)
			Dim oMGFtoDTA As clsDtaGenMSConvert

			oMGFtoDTA = New clsDtaGenMSConvert()
			oMGFtoDTA.Setup(GetDtaGenInitParams())

			oMGFtoDTA.ForceCentroidOn = True

			Dim eResult As IJobParams.CloseOutType
			eResult = StartAndWaitForDTAGenerator(oMGFtoDTA, "CentroidCDTA", True)

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return eResult
			End If

		Catch ex As Exception
			m_message = "Error creating a centroided _DTA.txt file in CentroidCDTA"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Try

			' Rename the new _DTA.txt file to _DTA_Centroided.txt

			Dim fiCDTA As FileInfo = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & CDTA_FILE_SUFFIX))
			If Not fiCDTA.Exists() Then
				m_message = "File not found in CentroidCDTA (after calling clsDtaGenMSConvert): " & fiCDTA.Name
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			End If

			PRISM.Processes.clsProgRunner.GarbageCollectNow()
			Threading.Thread.Sleep(250)

			strCDTAFileCentroided = Path.Combine(m_WorkDir, m_Dataset & "_DTA_Centroided.txt")
			fiCDTA.MoveTo(strCDTAFileCentroided)

			m_jobParams.AddResultFileToSkip(fiCDTA.Name)

		Catch ex As Exception
			m_message = "Error renaming the centroided _DTA.txt file in CentroidCDTA"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Try
			' Read _DTA_Original.txt and _DTA_Centroided.txt in parallel
			' Create the final _DTA.txt file

			Dim blnSuccess As Boolean
			strCDTAFileFinal = Path.Combine(m_WorkDir, m_Dataset & CDTA_FILE_SUFFIX)

			blnSuccess = MergeCDTAs(strCDTAFileOriginal, strCDTAFileCentroided, strCDTAFileFinal)
			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then m_message = "MergeCDTAs returned False in CentroidCDTA"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			m_message = "Error creating final _DTA.txt file in CentroidCDTA"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Concatenates DTA files into a single test file
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Function ConcatSpectraFiles() As IJobParams.CloseOutType

		' Packages dta files into concatenated text files

		' Make sure at least one .dta file was created
		Dim diWorkDir As DirectoryInfo = New DirectoryInfo(m_WorkDir)
		Dim intDTACount As Integer = diWorkDir.GetFiles("*.dta").Length

		If intDTACount = 0 Then
			m_message = "No .DTA files were created"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " for job " & m_JobNum)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
		ElseIf m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Concatenating spectra files, job " & m_JobNum & ", step " & m_StepNum)
		End If


		Dim ConcatTools As New clsConcatToolWrapper(diWorkDir.FullName)

		If Not ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_DTA, m_Dataset) Then
			m_message = clsGlobal.AppendToComment(m_message, "Error packaging results: " & ConcatTools.ErrMsg)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum & ", step " & m_StepNum)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Make sure the _dta.txt file is retained
		m_jobParams.RemoveResultFileToSkip(m_Dataset & "_dta.txt")

		' Skip any .dta files
		m_jobParams.AddResultFileExtensionToSkip(".dta")

		' Try to save whatever files are in the work directory
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

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

	Protected Function GetMSConvertAppPath() As String

		Dim ProteoWizardDir As String = m_mgrParams.GetParam("ProteoWizardDir")			' MSConvert.exe is stored in the ProteoWizard folder
		Dim progLoc As String = Path.Combine(ProteoWizardDir, clsDtaGenMSConvert.MSCONVERT_FILENAME)

		Return progLoc

	End Function

	''' <summary>
	''' Deletes .raw files from working directory
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks>Overridden for other types of input files</remarks>
	Protected Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .raw file from the working directory
		Dim lstFilesToDelete As List(Of String)

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.DeleteDataFile, executing method")
		End If

		'Delete the .raw file
		Try
			lstFilesToDelete = New List(Of String)
			lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" & clsAnalysisResources.DOT_RAW_EXTENSION))
			lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" & clsAnalysisResources.DOT_MZXML_EXTENSION))
			lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" & clsAnalysisResources.DOT_MZML_EXTENSION))
			lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" & clsAnalysisResources.DOT_MGF_EXTENSION))

			For Each MyFile As String In lstFilesToDelete
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner.DeleteDataFile, deleting file " & MyFile)
				End If
				DeleteFileWithRetries(MyFile)
			Next
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting .raw file, job " & m_JobNum & ", step " & m_StepNum & Err.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Protected Function MergeCDTAs(ByVal strCDTAWithParentIonData As String, ByVal strCDTAWithFragIonData As String, ByVal strCDTAFileFinal As String) As Boolean

		Dim strMsMsDataList() As String = Nothing
		Dim intMsMsDataCount As Integer
		Dim strDataLinesToAppend As String

		Dim strMsMsDataListCentroid() As String = Nothing
		Dim intMsMsDataCountCentroid As Integer

		Dim udtParentIonDataHeader As clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType = New clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType
		Dim udtFragIonDataHeader As clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType = New clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

		Dim blnNextSpectrumAvailable As Boolean
		Dim intSpectrumCountSkipped As Integer

		Try
			Dim oCDTAReaderParentIons As clsDtaTextFileReader
			oCDTAReaderParentIons = New clsDtaTextFileReader(False)
			If Not oCDTAReaderParentIons.OpenFile(strCDTAWithParentIonData) Then
				m_message = "Error opening CDTA file with the parent ion data"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			Dim oCDTAReaderFragIonData As clsDtaTextFileReader
			oCDTAReaderFragIonData = New clsDtaTextFileReader(True)
			If Not oCDTAReaderFragIonData.OpenFile(strCDTAWithFragIonData) Then
				m_message = "Error opening CDTA file with centroided spectra data"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			intSpectrumCountSkipped = 0
			Using swCDTAOut As StreamWriter = New StreamWriter(New FileStream(strCDTAFileFinal, FileMode.Create, FileAccess.Write, FileShare.Read))

				While oCDTAReaderParentIons.ReadNextSpectrum(strMsMsDataList, intMsMsDataCount, udtParentIonDataHeader)

					Do While Not ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader)
						blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(strMsMsDataListCentroid, intMsMsDataCountCentroid, udtFragIonDataHeader)
						If Not blnNextSpectrumAvailable Then Exit Do
					Loop

					blnNextSpectrumAvailable = ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader)
					If Not blnNextSpectrumAvailable Then
						' We never did find a match; this is unexpected
						' Try closing the FragIonData file, re-opening, and parsing again
						oCDTAReaderFragIonData.CloseFile()
						udtFragIonDataHeader = New clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

						PRISM.Processes.clsProgRunner.GarbageCollectNow()
						Threading.Thread.Sleep(250)

						oCDTAReaderFragIonData = New clsDtaTextFileReader(True)
						If Not oCDTAReaderFragIonData.OpenFile(strCDTAWithFragIonData) Then
							m_message = "Error re-opening CDTA file with the fragment ion data"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							Return False
						End If

						Do While Not ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader)
							blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(strMsMsDataListCentroid, intMsMsDataCountCentroid, udtFragIonDataHeader)
							If Not blnNextSpectrumAvailable Then Exit Do
						Loop

						blnNextSpectrumAvailable = ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader)
						If Not blnNextSpectrumAvailable Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MergeCDTAs could not find spectrum with StartScan=" & udtParentIonDataHeader.ScanNumberStart & " and EndScan=" & udtParentIonDataHeader.ScanNumberEnd & " for " & Path.GetFileName(strCDTAWithParentIonData))
							intSpectrumCountSkipped += 1
						End If
					End If

					If blnNextSpectrumAvailable Then
						swCDTAOut.WriteLine()
						swCDTAOut.WriteLine(udtParentIonDataHeader.SpectrumTitleWithCommentChars)
						swCDTAOut.WriteLine(udtParentIonDataHeader.ParentIonLineText)

						strDataLinesToAppend = RemoveTitleAndParentIonLines(oCDTAReaderFragIonData.GetMostRecentSpectrumFileText)

						If String.IsNullOrWhiteSpace(strDataLinesToAppend) Then
							m_message = "oCDTAReaderFragIonData.GetMostRecentSpectrumFileText returned empty text for StartScan=" & udtParentIonDataHeader.ScanNumberStart & " and EndScan=" & udtParentIonDataHeader.ScanNumberEnd & " in MergeCDTAs for " & Path.GetFileName(strCDTAWithParentIonData)
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							Return False
						Else
							swCDTAOut.Write(strDataLinesToAppend)
						End If

					End If

				End While

			End Using

			Try
				oCDTAReaderParentIons.CloseFile()
				oCDTAReaderFragIonData.CloseFile()
			Catch ex As Exception
				' Ignore errors here
			End Try

			If intSpectrumCountSkipped > 0 Then
				m_EvalMessage = "Skipped " & intSpectrumCountSkipped & " spectra in MergeCDTAs since they were not created by MSConvert"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
			End If

		Catch ex As Exception
			m_message = "Error merging CDTA files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return True
	End Function

	Protected Function RemoveTitleAndParentIonLines(ByVal strSpectrumText As String) As String

		Dim strLine As String
		Dim sbOutput As Text.StringBuilder = New Text.StringBuilder(strSpectrumText.Length)
		Dim blnPreviousLineWasTitleLine As Boolean = False

		Using trReader As StringReader = New StringReader(strSpectrumText)

			While trReader.Peek() > -1
				strLine = trReader.ReadLine()

				If strLine.StartsWith("=") Then
					' Skip this line
					blnPreviousLineWasTitleLine = True
				ElseIf blnPreviousLineWasTitleLine Then
					' Skip this line
					blnPreviousLineWasTitleLine = False
				ElseIf Not String.IsNullOrEmpty(strLine) Then
					' Data line; keep it
					sbOutput.AppendLine(strLine)
				End If

			End While
		End Using

		Return sbOutput.ToString()

	End Function

	Protected Function ScanHeadersMatch(ByVal udtParentIonDataHeader As clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, ByVal udtFragIonDataHeader As clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType) As Boolean

		If udtParentIonDataHeader.ScanNumberStart = udtFragIonDataHeader.ScanNumberStart Then
			If udtParentIonDataHeader.ScanNumberEnd = udtFragIonDataHeader.ScanNumberEnd Then
				Return True
			Else
				' MSConvert wrote out these headers for dataset Athal0503_26Mar12_Jaguar_12-02-26
				' 3160.0001.13.dta
				' 3211.0001.11.dta
				' 3258.0001.12.dta
				' 3259.0001.13.dta

				' Thus, allow a match if ScanNumberStart matches but ScanNumberEnd is less than ScanNumberStart
				If udtFragIonDataHeader.ScanNumberEnd < udtFragIonDataHeader.ScanNumberStart Then
					Return True
				End If
			End If
		End If

		Return False

	End Function

	Protected Function StartAndWaitForDTAGenerator(ByVal oDTAGenerator As clsDtaGen, ByVal strCallingFunction As String, ByVal blnSecondPass As Boolean) As IJobParams.CloseOutType

		Dim RetVal As ISpectraFileProcessor.ProcessStatus = oDTAGenerator.Start
		If RetVal = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
			m_message = "Error starting spectra processor: " & oDTAGenerator.ErrMsg
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner." & strCallingFunction & ": " & m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner." & strCallingFunction & ": Spectra generation started")
		End If

		' Loop until the spectra generator finishes
		While (oDTAGenerator.Status = ISpectraFileProcessor.ProcessStatus.SF_STARTING) Or _
		   (oDTAGenerator.Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING)

			If blnSecondPass Then
				m_progress = CENTROID_CDTA_PROGRESS_START + oDTAGenerator.Progress * (100.0! - CENTROID_CDTA_PROGRESS_START) / 100.0!
			Else
				If m_CentroidDTAs Then
					m_progress = oDTAGenerator.Progress * (CENTROID_CDTA_PROGRESS_START / 100.0!)
				Else
					m_progress = oDTAGenerator.Progress
				End If
			End If


			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, oDTAGenerator.SpectraFileCount, "", "", "", False)
			Threading.Thread.Sleep(5000)				 'Delay for 5 seconds
		End While

		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, oDTAGenerator.SpectraFileCount, "", "", "", False)

		'Check for reason spectra generator exited
		If oDTAGenerator.Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner." & strCallingFunction & ": Error making DTA files: " & oDTAGenerator.ErrMsg)
			m_message = clsGlobal.AppendToComment(m_message, "Error making DTA files in " & strCallingFunction & ": " & oDTAGenerator.ErrMsg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		ElseIf oDTAGenerator.Results = ISpectraFileProcessor.ProcessResults.SF_ABORTED Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenToolRunner." & strCallingFunction & ": DTA generation aborted")
			m_message = clsGlobal.AppendToComment(m_message, "DTA generation aborted in " & strCallingFunction & "")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If oDTAGenerator.Results = ISpectraFileProcessor.ProcessResults.SF_NO_FILES_CREATED Then
			m_message = clsGlobal.AppendToComment(m_message, "No spectra files created in " & strCallingFunction & "")
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsDtaGenToolRunner." & strCallingFunction & ": " & m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
		Else
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenToolRunner." & strCallingFunction & ": Spectra generation completed")
			End If
		End If

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strDtaGeneratorAppPath As String, ByVal eDtaGenerator As eDTAGeneratorConstants) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim fiDtaGenerator As FileInfo
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		fiDtaGenerator = New FileInfo(strDtaGeneratorAppPath)
		If Not fiDtaGenerator.Exists Then
			Try
				m_message = "DtaGenerator not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strDtaGeneratorAppPath)
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

		End If

		' Store strDtaGeneratorAppPath in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(fiDtaGenerator)

		If eDtaGenerator = eDTAGeneratorConstants.DeconConsole OrElse eDtaGenerator = eDTAGeneratorConstants.DeconMSn Then
			' Lookup the version of the DeconConsole application
			Dim strDllPath As String

			blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiDtaGenerator.FullName)
			If Not blnSuccess Then Return False

			If eDtaGenerator = eDTAGeneratorConstants.DeconMSn Then
				' Legacy DeconMSn
				strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconMSnEngine.dll")
				ioToolFiles.Add(New FileInfo(strDllPath))
				blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDllPath)
				If Not blnSuccess Then Return False

			ElseIf eDtaGenerator = eDTAGeneratorConstants.DeconConsole Then
				' DeconConsole re-implementation of DeconMSn

				' Lookup the version of the DeconTools Backend (in the DeconTools folder)
				' In addition, add it to ioToolFiles
				strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconTools.Backend.dll")
				ioToolFiles.Add(New FileInfo(strDllPath))
				blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDllPath)
				If Not blnSuccess Then Return False


				' Lookup the version of DeconEngineV2 (in the DeconTools folder)
				strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconEngineV2.dll")
				blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDllPath)
				If Not blnSuccess Then Return False
			End If

		End If


		' Possibly also store the MSConvert version
		If m_CentroidDTAs Then
			ioToolFiles.Add(New FileInfo(GetMSConvertAppPath()))
		End If

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	Protected Function StoreToolVersionInfoDLL(ByVal strDtaGeneratorDLLPath As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of the DLL
		MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDtaGeneratorDLLPath)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(strDtaGeneratorDLLPath))

		' Possibly also store the MSConvert version
		If m_CentroidDTAs Then
			ioToolFiles.Add(New FileInfo(GetMSConvertAppPath()))
		End If

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Zips concatenated DTA file to reduce size
	''' </summary>
	''' <returns>CloseoutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Function ZipConcDtaFile() As IJobParams.CloseOutType

		'Zips the concatenated dta file
		Dim DtaFileName As String = m_Dataset & "_dta.txt"
		Dim DtaFilePath As String = Path.Combine(m_WorkDir, DtaFileName)

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Zipping concatenated spectra file, job " & m_JobNum & ", step " & m_StepNum)

		'Verify file exists
		If Not File.Exists(DtaFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to find concatenated dta file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Zip the file
		Try
			If Not MyBase.ZipFile(DtaFilePath, False) Then
				Dim Msg As String = "Error zipping concat dta file, job " & m_JobNum & ", step " & m_StepNum
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		Catch ex As Exception
			Dim Msg As String = "Exception zipping concat dta file, job " & m_JobNum & ", step " & m_StepNum & ": " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function
#End Region

End Class
