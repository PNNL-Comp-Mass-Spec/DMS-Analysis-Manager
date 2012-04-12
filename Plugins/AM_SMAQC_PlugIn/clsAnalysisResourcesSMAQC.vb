Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesSMAQC
	Inherits clsAnalysisResources

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strDatasetName As String
		strDatasetName = m_jobParams.GetParam("DatasetNum")

		' Retrieve the parameter file
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")

		If Not RetrieveFile(strParamFileName, strParamFileStoragePath, m_WorkingDir) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		' Retrieve the X!Tandem _xt.txt file
		If Not RetrieveXTandemFiles(strDatasetName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Retrieve the MASIC ScanStats.txt, ScanStatsEx.txt, and _SICstats.txt files
		If Not RetrieveMASICFiles(strDatasetName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function RetrieveMASICFiles(ByVal strDatasetName As String) As Boolean

		Dim CreateStoragePathInfoFile As Boolean = False

		Dim strMASICResultsFolderName As String = String.Empty
		strMASICResultsFolderName = m_jobParams.GetParam("MASIC_Results_Folder_Name")

		m_JobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX)
		m_JobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt")
		m_JobParams.AddResultFileExtensionToSkip("_SICstats.txt")

		If String.IsNullOrEmpty(strMASICResultsFolderName) Then
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the MASIC files by searching for any valid MASIC folder")
			End If

			Return RetrieveScanAndSICStatsFiles(m_WorkingDir, True, CreateStoragePathInfoFile)

		Else
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the MASIC files from " & strMASICResultsFolderName)
			End If

			Dim ServerPath As String
			ServerPath = FindValidFolder(strDatasetName, "", strMASICResultsFolderName, 2)

			If String.IsNullOrEmpty(ServerPath) Then
				m_message = "Dataset folder path not defined"
			Else

				Dim diFolderInfo As System.IO.DirectoryInfo
				Dim diMASICFolderInfo As System.IO.DirectoryInfo
				diFolderInfo = New System.IO.DirectoryInfo(ServerPath)

				If Not diFolderInfo.Exists Then
					m_message = "Dataset folder not found: " & diFolderInfo.FullName
				Else

					'See if the ServerPath folder actually contains a subfolder named strMASICResultsFolderName
					diMASICFolderInfo = New System.IO.DirectoryInfo(System.IO.Path.Combine(diFolderInfo.FullName, strMASICResultsFolderName))

					If Not diMASICFolderInfo.Exists Then
						m_message = "Unable to find MASIC results folder " & strMASICResultsFolderName
					Else

						Return RetrieveScanAndSICStatsFiles(m_WorkingDir, diMASICFolderInfo.FullName, True, CreateStoragePathInfoFile)

					End If
				End If
			End If
		End If

		Return False

	End Function

	Protected Function RetrieveXTandemFiles(ByVal strDatasetName As String) As Boolean

		Dim lstFileNamesToGet As New System.Collections.Generic.List(Of String)

		m_JobParams.AddResultFileExtensionToSkip("_xt.txt")

		' The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
		' For example, for dataset QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 using Special_Processing of
		'   SourceJob:Auto{Tool = "XTandem" AND Settings_File = "IonTrapDefSettings.xml" AND [Parm File] = "xtandem_Rnd1PartTryp.xml"}
		' leads to the input folder being XTM201009211859_Auto625059

		Dim strInputFolder As String
		strInputFolder = m_jobParams.GetParam("StepParameters", "InputFolderName")

		If String.IsNullOrEmpty(strInputFolder) Then
			m_message = "InputFolder step parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False

		ElseIf Not strInputFolder.ToUpper().StartsWith("XTM") Then
			m_message = "InputFolder is not an X!Tandem folder; it should start with XTM and is auto-determined by the SourceJob SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the X!Tandem files")
		End If

		lstFileNamesToGet.Add(strDatasetName & "_xt.txt")
		lstFileNamesToGet.Add(strDatasetName & "_xt_ResultToSeqMap.txt")
		lstFileNamesToGet.Add(strDatasetName & "_xt_SeqToProteinMap.txt")

		For Each FileToGet As String In lstFileNamesToGet

			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return False
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)

		Next

		Return True

	End Function
End Class
