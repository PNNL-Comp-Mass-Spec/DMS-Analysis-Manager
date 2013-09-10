Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesLipidMapSearch
	Inherits clsAnalysisResources

	Public Const DECONTOOLS_PEAKS_FILE_SUFFIX As String = "_peaks.txt"

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Retrieve the parameter file
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")

		If Not RetrieveFile(strParamFileName, strParamFileStoragePath) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		If Not RetrievePNNLOmicsResourceFiles("LipidToolsProgLoc") Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		' Retrieve the .Raw file and _Peaks.txt file for this dataset
		If Not RetrieveFirstDatasetFiles() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Potentially retrieve the .Raw file and _Peaks.txt file for the second dataset to be used by this job
		If Not RetrieveSecondDatasetFiles() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function RetrieveFirstDatasetFiles() As Boolean

		m_jobParams.AddResultFileExtensionToSkip(DECONTOOLS_PEAKS_FILE_SUFFIX)
		m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)

		' The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
		' For example, for dataset XG_lipid_pt5a using Special_Processing of
		'   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml"}, Job2:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml" AND Dataset LIKE "$Replace($ThisDataset,_Pos,)%NEG"}'
		' Gives these parameters:

		' SourceJob                     = 852150
		' InputFolderName               = "DLS201206180954_Auto852150"
		' DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
		' DatasetArchivePath            = \\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

		' SourceJob2                    = 852151
		' SourceJob2Dataset             = "XG_lipid_pt5aNeg"
		' SourceJob2FolderPath          = "\\proto-3\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"
		' SourceJob2FolderPathArchive   = "\\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"

		Dim strDatasetName As String
		strDatasetName = m_jobParams.GetParam("DatasetNum")

		Dim strDeconToolsFolderName As String
		strDeconToolsFolderName = m_jobParams.GetParam("StepParameters", "InputFolderName")

		If String.IsNullOrEmpty(strDeconToolsFolderName) Then
			m_message = "InputFolderName step parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False

		ElseIf Not strDeconToolsFolderName.ToUpper().StartsWith("DLS") Then
			m_message = "InputFolderName step parameter is not a DeconTools folder; it should start with DLS and is auto-determined by the SourceJob SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If


		Dim strDatasetFolder As String
		Dim strDatasetFolderArchive As String

		strDatasetFolder = m_jobParams.GetParam("JobParameters", "DatasetStoragePath")
		strDatasetFolderArchive = m_jobParams.GetParam("JobParameters", "DatasetArchivePath")

		If String.IsNullOrEmpty(strDatasetFolder) Then
			m_message = "DatasetStoragePath job parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		ElseIf String.IsNullOrEmpty(strDatasetFolderArchive) Then
			m_message = "DatasetArchivePath job parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		strDatasetFolder = System.IO.Path.Combine(strDatasetFolder, strDatasetName)
		strDatasetFolderArchive = System.IO.Path.Combine(strDatasetFolderArchive, strDatasetName)

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the dataset's .Raw file and DeconTools _peaks.txt file")
		End If

		Return RetrieveDatasetAndPeaksFile(strDatasetName, strDatasetFolder, strDatasetFolderArchive, strDeconToolsFolderName)

	End Function

	Protected Function RetrieveSecondDatasetFiles() As Boolean

		' The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
		' For example, for dataset XG_lipid_pt5a using Special_Processing of
		'   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml"}, Job2:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "LTQ_FT_Lipidomics_2012-04-16.xml" AND Dataset LIKE "$Replace($ThisDataset,_Pos,)%NEG"}'
		' Gives these parameters:

		' SourceJob                     = 852150
		' InputFolderName               = "DLS201206180954_Auto852150"
		' DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
		' DatasetArchivePath            = \\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

		' SourceJob2                    = 852151
		' SourceJob2Dataset             = "XG_lipid_pt5aNeg"
		' SourceJob2FolderPath          = "\\proto-3\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"
		' SourceJob2FolderPathArchive   = "\\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1\XG_lipid_pt5aNeg\DLS201206180955_Auto852151"

		Dim strSourceJob2 As String = m_jobParams.GetParam("JobParameters", "SourceJob2")
		Dim intSourceJob2 As Integer

		If String.IsNullOrWhiteSpace(strSourceJob2) Then
			' Second dataset is not defined; that's OK
			Return True
		End If

		If Not Integer.TryParse(strSourceJob2, intSourceJob2) Then
			m_message = "SourceJob2 is not numeric"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If intSourceJob2 <= 0 Then
			' Second dataset is not defined; that's OK
			Return True
		End If

		Dim strDataset2 As String
		strDataset2 = m_jobParams.GetParam("JobParameters", "SourceJob2Dataset")
		If String.IsNullOrEmpty(strDataset2) Then
			m_message = "SourceJob2Dataset job parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		Dim strInputFolder As String
		Dim strInputFolderArchive As String

		Dim diInputFolder As System.IO.DirectoryInfo
		Dim diInputFolderArchive As System.IO.DirectoryInfo

		strInputFolder = m_jobParams.GetParam("JobParameters", "SourceJob2FolderPath")
		strInputFolderArchive = m_jobParams.GetParam("JobParameters", "SourceJob2FolderPathArchive")

		If String.IsNullOrEmpty(strInputFolder) Then
			m_message = "SourceJob2FolderPath job parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		ElseIf String.IsNullOrEmpty(strInputFolderArchive) Then
			m_message = "SourceJob2FolderPathArchive job parameter not found; this is unexpected"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		diInputFolder = New System.IO.DirectoryInfo(strInputFolder)
		diInputFolderArchive = New System.IO.DirectoryInfo(strInputFolderArchive)

		If Not diInputFolder.Name.ToUpper().StartsWith("DLS") Then
			m_message = "SourceJob2FolderPath is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		ElseIf Not diInputFolderArchive.Name.ToUpper().StartsWith("DLS") Then
			m_message = "SourceJob2FolderPathArchive is not a DeconTools folder; the last folder should start with DLS and is auto-determined by the SourceJob2 SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the second dataset's .Raw file and DeconTools _peaks.txt file")
		End If

		Return RetrieveDatasetAndPeaksFile(strDataset2, diInputFolder.Parent.FullName, diInputFolderArchive.Parent.FullName, diInputFolder.Name)
	
	End Function

	Protected Function RetrieveDatasetAndPeaksFile(ByVal strDatasetName As String, ByVal strDatasetFolderPath As String, ByVal strDatasetFolderPathArchive As String, ByVal strDeconToolsFolderName As String) As Boolean

		Dim strFileToFind As String

		' Copy the .Raw file
		' Search the dataset folder first, then the archive folder

		strFileToFind = strDatasetName & DOT_RAW_EXTENSION
		If Not CopyFileToWorkDir(strFileToFind, strDatasetFolderPath, m_WorkingDir, clsLogTools.LogLevels.INFO) Then
			' Raw file not found on the storage server; try the archive
			If Not CopyFileToWorkDir(strFileToFind, strDatasetFolderPathArchive, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
				' Raw file still not found; try MyEMSL

				Dim DSFolderPath As String = FindValidFolder(strDatasetName, strFileToFind)
				If DSFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
					' Queue this file for download
					m_MyEMSLDatasetInfo.AddFileToDownloadQueue(m_RecentlyFoundMyEMSLFiles.First().FileID)
				Else
					' Raw file still not found; abort processing
					Return False
				End If
				
			End If
		End If

		' As of January 2013, the _peaks.txt file generated by DeconTools does not have accurate data for centroided spectra
		' Therefore, rather than copying the _Peaks.txt file locally, we will allow the LipidTools.exe software to re-generate it

		m_jobParams.AddResultFileExtensionToSkip(DECONTOOLS_PEAKS_FILE_SUFFIX)

		'If False Then

		'	' Copy the _Peaks.txt file
		'	' For jobs run after August 23, 2012, this file will be zipped

		'	Dim blnUnzipPeaksFile As Boolean = False
		'	strFileToFind = IO.Path.ChangeExtension(strDatasetName & DECONTOOLS_PEAKS_FILE_SUFFIX, "zip")

		'	If CopyFileToWorkDir(strFileToFind, System.IO.Path.Combine(strDatasetFolderPath, strDeconToolsFolderName), m_WorkingDir, clsLogTools.LogLevels.INFO) Then
		'		blnUnzipPeaksFile = True
		'	Else
		'		' _Peaks.zip file not found on the storage server; try the archive
		'		If CopyFileToWorkDir(strFileToFind, System.IO.Path.Combine(strDatasetFolderPathArchive, strDeconToolsFolderName), m_WorkingDir, clsLogTools.LogLevels.INFO) Then
		'			blnUnzipPeaksFile = True
		'		Else
		'			' _Peaks.zip file still not found; this is OK, since LipidTools.exe can generate it using the .Raw file
		'		End If
		'	End If

		'	If blnUnzipPeaksFile Then
		'		m_jobParams.AddResultFileToSkip(strFileToFind)
		'		UnzipFileStart(IO.Path.Combine(m_WorkingDir, strFileToFind), m_WorkingDir, "RetrieveDatasetAndPeaksFile", False)
		'	End If
		'End If

		Return True

	End Function
End Class
