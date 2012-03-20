'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGFDB
	Inherits clsAnalysisResources

	Private WithEvents mCDTACondenser As CondenseCDTAFile.clsCDTAFileCondenser
	Private WithEvents mMSFileInfoScanner As MSFileInfoScanner.clsMSFileInfoScanner

	Private mMSFileInfoScannerErrorCount As Integer

	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

		Dim strDatasetName As String
		strDatasetName = m_jobParams.GetParam("DatasetNum")

		'Clear out list of files to delete or keep when packaging the results
		clsGlobal.ResetFilesToDeleteOrKeep()

		' Make sure the machine has enough free memory to run MSGFDB
		If Not ValidateFreeMemorySize("MSGFDBJavaMemorySize", "MSGFDB", False) Then
			m_message = "Not enough free memory to run MSGFDB"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Retrieve Fasta file
		If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

		' Retrieve param file
		' This will also obtain the _ModDefs.txt file using query 
		'  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
		'  FROM V_Param_File_Mass_Mod_Info 
		'  WHERE Param_File_Name = 'ParamFileName'
		If Not RetrieveGeneratedParamFile( _
		   m_jobParams.GetParam("ParmFileName"), _
		   m_jobParams.GetParam("ParmFileStoragePath"), _
		   m_mgrParams.GetParam("workdir")) _
		Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' The ToolName job parameter holds the name of the job script we are executing
		Dim strScriptName As String = m_jobParams.GetParam("ToolName")

		If strScriptName.ToLower().Contains("mzxml") OrElse strScriptName.ToLower().Contains("msgfdb_bruker") Then

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting mzXML file")

			' Retrieve the .mzXML file for this dataset
			' Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
			' Instead, use FindAndRetrieveMiscFiles 

			' Note that capitalization matters for the extension; it must be .mzXML
			Dim FileToGet As String = m_jobParams.GetParam("DatasetNum") & ".mzXML"
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)

		Else
			' Retrieve the _DTA.txt file
			' Retrieve unzipped dta files (do not de-concatenate since MSGFDB uses the _Dta.txt file directly)
			If Not RetrieveDtaFiles(False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim strAssumedScanType As String
			strAssumedScanType = m_jobParams.GetParam("AssumedScanType")

			If Not String.IsNullOrWhiteSpace(strAssumedScanType) Then
				' Scan type is assumed; we don't need the Masic ScanStats.txt files or the .Raw file
				Select Case strAssumedScanType.ToUpper()
					Case "CID", "ETD", "HCD"
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Assuming scan type is '" & strAssumedScanType & "'")
						End If
					Case Else
						m_message = "Invalid assumed scan type '" & strAssumedScanType & "'; must be CID, ETD, or HCD"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Select
			Else
				' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
				If Not RetrieveScanStatsFiles(m_WorkingDir, False) Then
					' _ScanStats.txt file not found
					' If processing a .Raw file or .UIMF file then we can create the file using the MSFileInfoScanner
					If Not GenerateScanStatsFile(strDatasetName) Then
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				Else
					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieved MASIC ScanStats and ScanStatsEx files")
					End If
				End If
			End If

			' If the _dta.txt file is over 2 GB in size, then condense it
			If Not ValidateDTATextFileSize(m_WorkingDir, strDatasetName & "_dta.txt") Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		End If


		'Add all the extensions of the files to delete after run
		clsGlobal.m_FilesToDeleteExt.Add(".mzXML")
		clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
		clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
		clsGlobal.m_FilesToDeleteExt.Add("temp.tsv") ' MSGFDB creates .txt.temp.tsv files, which we don't need

		clsGlobal.m_FilesToDeleteExt.Add(SCAN_STATS_FILE_SUFFIX)
		clsGlobal.m_FilesToDeleteExt.Add("_ScanStatsEx.txt")

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function GenerateScanStatsFile(strDatasetName As String) As Boolean

		Dim strRawDataType As String
		Dim strInputFilePath As String = String.Empty

		Dim blnSuccess As Boolean

		Try
			' Confirm that this dataset is a Thermo .Raw file or a .UIMF file

			strRawDataType = m_jobParams.GetParam("RawDataType")

			Select Case strRawDataType.ToLower
				Case RAW_DATA_TYPE_DOT_RAW_FILES
					strInputFilePath = strDatasetName & DOT_RAW_EXTENSION
				Case RAW_DATA_TYPE_DOT_UIMF_FILES
					strInputFilePath = strDatasetName & DOT_UIMF_EXTENSION
				Case Else
					m_message = "Invalid dataset type for auto-generating ScanStats.txt file: " & strRawDataType
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " & m_message)
					Return False

			End Select

			strInputFilePath = System.IO.Path.Combine(m_WorkingDir, strInputFilePath)

			If Not RetrieveSpectra(strRawDataType, m_WorkingDir) Then
				Dim strExtraMsg As String = m_message
				m_message = "Error retrieving spectra file"
				If Not String.isnullorwhitespace(strExtraMsg) Then
					m_message &= "; " & strExtraMsg
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message)
				Return False
			End If

			' Make sure the raw data file does not get copied to the results folder
			clsGlobal.FilesToDelete.Add(System.IO.Path.GetFileName(strInputFilePath))

			mMSFileInfoScannerErrorCount = 0
			mMSFileInfoScanner = New MSFileInfoScanner.clsMSFileInfoScanner()

			mMSFileInfoScanner.CheckFileIntegrity = False
			mMSFileInfoScanner.CreateDatasetInfoFile = False
			mMSFileInfoScanner.CreateScanStatsFile = True
			mMSFileInfoScanner.SaveLCMS2DPlots = False
			mMSFileInfoScanner.SaveTICAndBPIPlots = False
			mMSFileInfoScanner.UpdateDatasetStatsTextFile = False

			blnSuccess = mMSFileInfoScanner.ProcessMSFileOrFolder(strInputFilePath, m_WorkingDir)

			If blnSuccess Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generated ScanStats file using " & strInputFilePath)
				End If
			Else
				m_message = "Error generating ScanStats file using " & strInputFilePath
				Dim strMsgAddnl As String = mMSFileInfoScanner.GetErrorMessage

				If Not String.IsNullOrEmpty(strMsgAddnl) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strMsgAddnl)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End If

			End If

			System.Threading.Thread.Sleep(500)
			Try
				System.IO.File.Delete(strInputFilePath)
			Catch ex As Exception
				' Ignore errors here
			End Try

		Catch ex As Exception
			m_message = "Exception in GenerateScanStatsFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function ValidateDTATextFileSize(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean
		Const FILE_SIZE_THRESHOLD As Integer = Int32.MaxValue

		Dim ioFileInfo As System.IO.FileInfo
		Dim strInputFilePath As String
		Dim strFilePathOld As String

		Dim strMessage As String

		Dim blnSuccess As Boolean

		Try
			strInputFilePath = System.IO.Path.Combine(strWorkDir, strInputFileName)
			ioFileInfo = New System.IO.FileInfo(strInputFilePath)

			If Not ioFileInfo.Exists Then
				m_message = "_DTA.txt file not found: " & strInputFilePath
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If ioFileInfo.Length >= FILE_SIZE_THRESHOLD Then
				' Need to condense the file

				strMessage = ioFileInfo.Name & " is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB in size; will now condense it by combining data points with consecutive zero-intensity values"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)

				mCDTACondenser = New CondenseCDTAFile.clsCDTAFileCondenser

				blnSuccess = mCDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName)

				If Not blnSuccess Then
					m_message = "Error condensing _DTA.txt file: " & mCDTACondenser.GetErrorMessage()
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				Else
					' Wait 500 msec, then check the size of the new _dta.txt file
					System.Threading.Thread.Sleep(500)

					ioFileInfo.Refresh()

					If m_DebugLevel >= 1 Then
						strMessage = "Condensing complete; size of the new _dta.txt file is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
					End If

					Try
						strFilePathOld = System.IO.Path.Combine(strWorkDir, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.FullName) & "_Old.txt")

						If m_DebugLevel >= 2 Then
							strMessage = "Now deleting file " & strFilePathOld
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
						End If

						ioFileInfo = New System.IO.FileInfo(strFilePathOld)
						If ioFileInfo.Exists Then
							ioFileInfo.Delete()
						Else
							strMessage = "Old _DTA.txt file not found:" & ioFileInfo.FullName & "; cannot delete"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
						End If

					Catch ex As Exception
						' Error deleting the file; log it but keep processing
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception deleting _dta_old.txt file: " & ex.Message)
					End Try

				End If
			End If

			blnSuccess = True

		Catch ex As Exception
			m_message = "Exception in ValidateDTATextFileSize"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

	Private Sub mCDTACondenser_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mCDTACondenser.ProgressChanged
		Static dtLastUpdateTime As System.DateTime

		If m_DebugLevel >= 1 Then
			If m_DebugLevel = 1 AndAlso System.DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 60 OrElse _
			   m_DebugLevel > 1 AndAlso System.DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
				dtLastUpdateTime = System.DateTime.UtcNow

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & percentComplete.ToString("0.00") & "% complete")
			End If
		End If
	End Sub

	Private Sub mMSFileInfoScanner_ErrorEvent(Message As String) Handles mMSFileInfoScanner.ErrorEvent
		mMSFileInfoScannerErrorCount += 1
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSFileInfoScanner error: " & Message)
	End Sub

	Private Sub mMSFileInfoScanner_MessageEvent(Message As String) Handles mMSFileInfoScanner.MessageEvent
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & Message)
		End If
	End Sub
End Class
