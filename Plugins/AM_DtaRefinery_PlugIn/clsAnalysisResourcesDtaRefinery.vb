Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesDtaRefinery
    Inherits clsAnalysisResources

    Friend Const XTANDEM_DEFAULT_INPUT_FILE As String = "xtandem_default_input.xml"
    Friend Const XTANDEM_TAXONOMY_LIST_FILE As String = "xtandem_taxonomy_list.xml"
    Friend Const DTA_REFINERY_INPUT_FILE As String = "DtaRefinery_input.xml"
    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim result As Boolean
        Dim strErrorMessage As String

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'This will eventually be replaced by Ken Auberry dll call to make param file on the fly

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

		'Retrieve param file
		Dim strParamFileName = m_jobParams.GetParam("ParmFileName")

		If Not RetrieveGeneratedParamFile(strParamFileName, m_jobParams.GetParam("ParmFileStoragePath")) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Dim strParamFileStoragePathKeyName As String
		Dim strDtaRefineryParmFileStoragePath As String
		strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "DTA_Refinery"

		strDtaRefineryParmFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
		If strDtaRefineryParmFileStoragePath Is Nothing OrElse strDtaRefineryParmFileStoragePath.Length = 0 Then
			strDtaRefineryParmFileStoragePath = "\\gigasax\dms_parameter_Files\DTARefinery"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strDtaRefineryParmFileStoragePath)
		End If

		'Retrieve settings files aka default file that will have values overwritten by parameter file values
		'Stored in same location as parameter file
		If Not RetrieveFile(XTANDEM_DEFAULT_INPUT_FILE, strDtaRefineryParmFileStoragePath) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not RetrieveFile(XTANDEM_TAXONOMY_LIST_FILE, strDtaRefineryParmFileStoragePath) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not RetrieveFile(m_jobParams.GetParam("DTARefineryXMLFile"), strDtaRefineryParmFileStoragePath) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Retrieve the _DTA.txt file
		' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
		If Not RetrieveDtaFiles() Then
			' Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
		Dim strCDTAPath As String = Path.Combine(m_WorkingDir, m_DatasetName & "_dta.txt")
		Dim blnReplaceSourceFile As Boolean = True
		Dim blnDeleteSourceFileIfUpdated As Boolean = True

		If Not ValidateCDTAFileScanAndCSTags(strCDTAPath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, "") Then
			m_message = "Error validating the _DTA.txt file"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' If the _dta.txt file is over 2 GB in size, then condense it
		If Not ValidateCDTAFileSize(m_WorkingDir, Path.GetFileName(strCDTAPath)) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Retrieve DeconMSn Log file and DeconMSn Profile File
		If Not RetrieveDeconMSnLogFiles() Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Add all the extensions of the files to delete after run
		m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
		m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
		m_jobParams.AddResultFileExtensionToSkip(".dta")  'DTA files
		m_jobParams.AddResultFileExtensionToSkip(m_DatasetName & ".xml")

		m_jobParams.AddResultFileToSkip(strParamFileName)
		m_jobParams.AddResultFileToSkip(Path.GetFileNameWithoutExtension(strParamFileName) & "_ModDefs.txt")
		m_jobParams.AddResultFileToSkip("Mass_Correction_Tags.txt")

		m_jobParams.AddResultFileToKeep(m_DatasetName & "_dta.zip")

		' Set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
		strErrorMessage = String.Empty
		result = UpdateParameterFile(strErrorMessage)
		If Not result Then
			Dim Msg As String = "clsAnalysisResourcesDtaRefinery.GetResources(), failed making input file: " & strErrorMessage
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function RetrieveDeconMSnLogFiles() As Boolean

		Dim sourceFolderPath As String

		Try
			Dim deconMSnLogFileName = m_DatasetName & "_DeconMSn_log.txt"
			sourceFolderPath = FindDataFile(deconMSnLogFileName)

			If String.IsNullOrWhiteSpace(sourceFolderPath) Then
				' Could not find the file (error will have already been logged)
				' We'll continue on, but log a warning
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Could not find the DeconMSn Log file named " & deconMSnLogFileName)
				End If
				deconMSnLogFileName = String.Empty
			Else
				If Not CopyFileToWorkDir(deconMSnLogFileName, sourceFolderPath, m_WorkingDir) Then
					' Error copying file (error will have already been logged)
					If m_DebugLevel >= 3 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & deconMSnLogFileName & " using folder " & sourceFolderPath)
					End If
					' Ignore the error and continue				
				End If
			End If


			Dim deconMSnProfileFileName = m_DatasetName & "_profile.txt"
			sourceFolderPath = FindDataFile(deconMSnProfileFileName)

			If String.IsNullOrWhiteSpace(sourceFolderPath) Then
				' Could not find the file (error will have already been logged)
				' We'll continue on, but log a warning
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Could not find the DeconMSn Profile file named " & deconMSnProfileFileName)
				End If
				deconMSnProfileFileName = String.Empty
			Else
				If Not CopyFileToWorkDir(deconMSnProfileFileName, sourceFolderPath, m_WorkingDir) Then
					' Error copying file (error will have already been logged)
					If m_DebugLevel >= 3 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & deconMSnProfileFileName & " using folder " & sourceFolderPath)
					End If
					' Ignore the error and continue
				End If
			End If

			If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
				Return False
			End If

			If Not String.IsNullOrWhiteSpace(deconMSnLogFileName) Then
				If Not ValidateDeconMSnLogFile(Path.Combine(m_WorkingDir, deconMSnLogFileName)) Then
					Return False
				End If
			End If

			DeleteFileIfNoData(deconMSnLogFileName, "DeconMSn Log file")

			DeleteFileIfNoData(deconMSnProfileFileName, "DeconMSn Profile file")

			' Make sure the DeconMSn files are not stored in the DTARefinery results folder
			m_jobParams.AddResultFileExtensionToSkip("_DeconMSn_log.txt")
			m_jobParams.AddResultFileExtensionToSkip("_profile.txt")

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RetrieveDeconMSnLogFiles: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Sub DeleteFileIfNoData(ByVal fileName As String, ByVal fileDescription As String)

		Dim strErrorMessage As String = String.Empty
		Dim strFilePathToCheck As String

		If Not String.IsNullOrWhiteSpace(fileName) Then
			strFilePathToCheck = Path.Combine(m_WorkingDir, fileName)
			If Not ValidateFileHasData(strFilePathToCheck, fileDescription, strErrorMessage) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, fileDescription & " does not have any tab-delimited lines that start with a number; file will be deleted so that DTARefinery can proceed without considering TIC or ion intensity")
				End If

				File.Delete(strFilePathToCheck)
			End If
		End If

	End Sub

	Protected Function UpdateParameterFile(ByRef strErrorMessage As String) As Boolean
		'ByVal strTemplateFilePath As String, ByVal strFileToMerge As String, 
		Dim XTandemExePath As String
		Dim XtandemDefaultInput As String = Path.Combine(m_WorkingDir, XTANDEM_DEFAULT_INPUT_FILE)
		Dim XtandemTaxonomyList As String = Path.Combine(m_WorkingDir, XTANDEM_TAXONOMY_LIST_FILE)
		Dim ParamFilePath As String = Path.Combine(m_WorkingDir, m_jobParams.GetParam("DTARefineryXMLFile"))
		Dim DtaRefineryDirectory As String = Path.GetDirectoryName(m_mgrParams.GetParam("dtarefineryloc"))

		Dim SearchSettings As String = Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

		Dim result As Boolean = True
		Dim fiTemplateFile As FileInfo
		Dim objTemplate As System.Xml.XmlDocument
		strErrorMessage = String.Empty

		Try
			fiTemplateFile = New FileInfo(ParamFilePath)

			If Not fiTemplateFile.Exists Then
				strErrorMessage = "File not found: " & fiTemplateFile.FullName
				Return False
			End If

			' Open the template XML file
			objTemplate = New System.Xml.XmlDocument
			objTemplate.PreserveWhitespace = True
			Try
				objTemplate.Load(fiTemplateFile.FullName)
			Catch ex As Exception
				strErrorMessage = "Error loading file " & fiTemplateFile.Name & ": " & ex.Message
				Return False
			End Try

			' Now override the values for xtandem parameters file
			Try
				Dim par As System.Xml.XmlNode
				Dim root As System.Xml.XmlElement = objTemplate.DocumentElement

				XTandemExePath = Path.Combine(DtaRefineryDirectory, "aux_xtandem_module\tandem_5digit_precision.exe")
				par = root.SelectSingleNode("/allPars/xtandemPars/par[@label='xtandem exe file']")
				par.InnerXml = XTandemExePath

				par = root.SelectSingleNode("/allPars/xtandemPars/par[@label='default input']")
				par.InnerXml = XtandemDefaultInput

				par = root.SelectSingleNode("/allPars/xtandemPars/par[@label='taxonomy list']")
				par.InnerXml = XtandemTaxonomyList

			Catch ex As Exception
				strErrorMessage = "Error updating the MSInFile nodes: " & ex.Message
				Return False
			End Try

			' Write out the new file
			objTemplate.Save(ParamFilePath)

		Catch ex As Exception
			strErrorMessage = "Error: " & ex.Message
			Return False
		End Try

		Return True

	End Function

	Private Function ValidateDeconMSnLogFile(ByVal strFilePath As String) As Boolean

		Dim oValidator As New clsDeconMSnLogFileValidator()
		Dim blnSuccess As Boolean

		blnSuccess = oValidator.ValidateDeconMSnLogFile(strFilePath)
		If Not blnSuccess Then
			If String.IsNullOrEmpty(oValidator.ErrorMessage) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDeconMSnLogFileValidator.ValidateFile returned false")
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, oValidator.ErrorMessage)
			End If
			Return False
		Else
			If oValidator.FileUpdated Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsDeconMSnLogFileValidator.ValidateFile updated one or more rows in the DeconMSn_Log.txt file to replace values with intensities of 0 with 1")
			End If
		End If

		Return True
	End Function

End Class
