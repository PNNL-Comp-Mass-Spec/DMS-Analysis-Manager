' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerICR
	Inherits clsAnalysisToolRunnerICRBase

    'Performs PEK analysis using ICR-2LS on Bruker S-folder MS data

    ' Example folder layout when processing S-folders 
    '
    ' C:\DMS_WorkDir1\   contains the .Par file
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\   is empty
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001   contains 100 files (see below)
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002   contains another 100 files (see below)
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s003
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s004
    ' C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s005
    ' etc.
    ' 
    ' Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001\
    ' 110409_His.00001
    ' 110409_His.00002
    ' 110409_His.00003
    ' ...
    ' 110409_His.00099
    ' 110409_His.00100
    ' 
    ' Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002\
    ' 110409_His.00101
    ' 110409_His.00102
    ' 110409_His.00103
    ' etc.
    ' 
    ' 

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim ResCode As IJobParams.CloseOutType
		Dim DSNamePath As String

		Dim UseAllScans As Boolean = True

		Dim SerFileOrFolderPath As String
		Dim blnIsFolder As Boolean = False

		Dim eICR2LSMode As ICR2LSProcessingModeConstants
		Dim strSerTypeName As String

		Dim OutFileNamePath As String
		Dim ParamFilePath As String
		Dim RawDataType As String
		Dim blnBrukerFT As Boolean
		Dim DatasetFolderPathBase As String

		Dim blnSuccess As Boolean

		Dim currentTask = "Initializing"

		Try

			'Start with base class function to get settings information
			ResCode = MyBase.RunTool()
			If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

			' Store the ICR2LS version info in the database
			currentTask = "StoreToolVersionInfo"

			If Not StoreToolVersionInfo() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining ICR2LS version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Verify a param file has been specified
			currentTask = "Verify param file path"
			ParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

			currentTask = "Verify param file path: " & ParamFilePath
			If Not File.Exists(ParamFilePath) Then
				'Param file wasn't specified, but is required for ICR-2LS analysis
				m_message = "ICR-2LS Param file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ParamFilePath)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Add handling of settings file info here if it becomes necessary in the future

			'Get scan settings from settings file
			Dim MinScan = m_jobParams.GetJobParameter("scanstart", 0)
			Dim MaxScan = m_jobParams.GetJobParameter("ScanStop", 0)

			' Determine whether or not we should be processing MS2 spectra
			Dim SkipMS2 = Not m_jobParams.GetJobParameter("ProcessMS2", False)

			If (MinScan = 0 AndAlso MaxScan = 0) OrElse _
			   MinScan > MaxScan OrElse _
			   MaxScan > 500000 Then
				UseAllScans = True
			Else
				UseAllScans = False
			End If

			'Assemble the dataset name
			DSNamePath = Path.Combine(m_WorkDir, m_Dataset)
			RawDataType = m_jobParams.GetParam("RawDataType")

			'Assemble the output file name and path
			OutFileNamePath = Path.Combine(m_WorkDir, m_Dataset & ".pek")

			' Determine the location of the ser file (or fid file)
			' It could be in a "0.ser" folder, a ser file inside a .D folder, or a fid file inside a .D folder

			If RawDataType.ToLower() = clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER Then
				DatasetFolderPathBase = Path.Combine(m_WorkDir, m_Dataset & ".d")
				blnBrukerFT = True
			Else
				DatasetFolderPathBase = String.Copy(m_WorkDir)
				blnBrukerFT = False
			End If

			' Look for a ser file or fid file in the working directory
			currentTask = "FindSerFileOrFolder"

			SerFileOrFolderPath = clsAnalysisResourcesIcr2ls.FindSerFileOrFolder(DatasetFolderPathBase, blnIsFolder)

			If String.IsNullOrEmpty(SerFileOrFolderPath) Then
				' Did not find a ser file, fid file, or 0.ser folder

				If blnBrukerFT Then
					m_message = "ser file or fid file not found; unable to process with ICR-2LS"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				Else
					' Assume we are processing zipped s-folders, and thus there should be a folder with the Dataset's name in the work directory
					'  and in that folder will be unzipped contents of the s-folders (one file per spectrum)

					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Did not find a ser file, fid file, or 0.ser folder; assuming we are processing zipped s-folders")
					End If
				End If
			Else
				If m_DebugLevel >= 1 Then
					If blnIsFolder Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "0.ser folder found: " & SerFileOrFolderPath)
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Path.GetFileName(SerFileOrFolderPath) & " file found: " & SerFileOrFolderPath)
					End If

				End If
			End If

			If Not String.IsNullOrEmpty(SerFileOrFolderPath) Then
				If Not blnIsFolder Then
					eICR2LSMode = ICR2LSProcessingModeConstants.SerFilePEK
					strSerTypeName = "file"
				Else
					eICR2LSMode = ICR2LSProcessingModeConstants.SerFolderPEK
					strSerTypeName = "folder"
				End If

				currentTask = "StartICR2LS for " & SerFileOrFolderPath
				blnSuccess = MyBase.StartICR2LS(SerFileOrFolderPath, ParamFilePath, OutFileNamePath, eICR2LSMode, UseAllScans, SkipMS2, MinScan, MaxScan)
				If Not blnSuccess Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS on " & strSerTypeName & " " & SerFileOrFolderPath)
				End If
			Else
				' Processing zipped s-folders
				If Not Directory.Exists(DSNamePath) Then
					m_message = "Data file folder not found: " & DSNamePath
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				currentTask = "StartICR2LS for zippsed s-folders in " & DSNamePath

				eICR2LSMode = ICR2LSProcessingModeConstants.SFoldersPEK
				blnSuccess = MyBase.StartICR2LS(DSNamePath, ParamFilePath, OutFileNamePath, eICR2LSMode, UseAllScans, SkipMS2, MinScan, MaxScan)

				If Not blnSuccess Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS on zipped s-files in " & DSNamePath)
				End If
			End If

			If Not blnSuccess Then
				' If a .PEK file exists, then call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
				currentTask = "VerifyPEKFileExists"
				If VerifyPEKFileExists(m_WorkDir, m_Dataset) Then
					m_message = "ICR-2LS returned false (see .PEK file in Failed results folder)"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, ".Pek file was found, so will save results to the failed results archive folder")

					PerfPostAnalysisTasks(False)

					' Try to save whatever files were moved into the results folder
					Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
					objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

				Else
					m_message = "Error running ICR-2LS (.Pek file not found in " & m_WorkDir & ")"
				End If

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				' Make sure the .pek file and .Par file are named properly
				currentTask = "FixICR2LSResultFileNames"
				FixICR2LSResultFileNames(m_WorkDir, m_Dataset)
			End If

			'Run the cleanup routine from the base class
			currentTask = "PerfPostAnalysisTasks"

			If PerfPostAnalysisTasks(True) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				m_message = clsGlobal.AppendToComment(m_message, "Error performing post analysis tasks")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error in clsAnalysisToolRunnerICR: " & ex.Message & "; task = " & currentTask)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try


	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the dataset folder containing s-folders from the working directory
		Dim RetryCount As Integer = 0
		Dim ErrMsg As String = String.Empty

		While RetryCount < 3
			Try
				System.Threading.Thread.Sleep(5000)				'Allow extra time for ICR2LS to release file locks
				If Directory.Exists(Path.Combine(m_WorkDir, m_Dataset)) Then
					Directory.Delete(Path.Combine(m_WorkDir, m_Dataset), True)
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Catch Err As IOException
				'If problem is locked file, retry
				If m_DebugLevel > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting data file, attempt #" & RetryCount.ToString)
				End If
				ErrMsg = Err.Message
				RetryCount += 1
			Catch Err As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting raw data files, job " & m_JobNum & ": " & Err.Message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		End While

		'If we got to here, then we've exceeded the max retry limit
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Unable to delete raw data file after multiple tries, job " & m_JobNum & ", Error " & ErrMsg)
		Return IJobParams.CloseOutType.CLOSEOUT_FAILED

	End Function

	''' <summary>
	''' Look for the .PEK and .PAR files in the specified folder
	''' Make sure they are named Dataset_m_dd_yyyy.PAR and Dataset_m_dd_yyyy.Pek
	''' </summary>
	''' <param name="strFolderPath">Folder to examine</param>
	''' <param name="strDatasetName">Dataset name</param>
	''' <remarks></remarks>
	Protected Sub FixICR2LSResultFileNames(ByVal strFolderPath As String, ByVal strDatasetName As String)

		Dim objExtensionsToCheck As New System.Collections.Generic.List(Of String)

		Dim fiFolder As DirectoryInfo
		Dim fiFile As FileInfo

		Dim strDSNameLCase As String
		Dim strExtension As String

		Dim strDesiredName As String

		Try

			objExtensionsToCheck.Add("PAR")
			objExtensionsToCheck.Add("Pek")

			strDSNameLCase = strDatasetName.ToLower()

			fiFolder = New DirectoryInfo(strFolderPath)

			If fiFolder.Exists Then
				For Each strExtension In objExtensionsToCheck

					For Each fiFile In fiFolder.GetFiles("*." & strExtension)
						If fiFile.Name.ToLower.StartsWith(strDSNameLCase) Then

							' Name should be of the form: Dataset_1_24_2010.PAR
							' The datestamp in the name is month_day_year
							strDesiredName = strDatasetName & "_" & System.DateTime.Now.ToString("M_d_yyyy") & "." & strExtension

							If fiFile.Name.ToLower <> strDesiredName.ToLower Then
								Try
									If m_DebugLevel >= 1 Then
										clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Renaming " & strExtension & " file from " & fiFile.Name & " to " & strDesiredName)
									End If

									fiFile.MoveTo(Path.Combine(fiFolder.FullName, strDesiredName))
								Catch ex As Exception
									' Rename failed; that means the correct file already exists; this is OK
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Rename failed: " & ex.Message)
								End Try

							End If

							Exit For
						End If
					Next fiFile

				Next strExtension
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in FixICR2LSResultFileNames; folder not found: " & strFolderPath)
			End If


		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in FixICR2LSResultFileNames: " & ex.Message)
		End Try

	End Sub

End Class
