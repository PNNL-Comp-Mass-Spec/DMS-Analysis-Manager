'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader
Imports System.IO

Public Class clsAnalysisResourcesMSGF
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Manages retrieval of all files needed by MSGF
	'*********************************************************************************************************

#Region "Constants"
	Public Const PHRP_MOD_DEFS_SUFFIX As String = "_ModDefs.txt"
#End Region

#Region "Module variables"
	' Keys are the original file name, values are the new name
	Protected m_PendingFileRenames As Dictionary(Of String, String)
#End Region

#Region "Methods"
	''' <summary>
	''' Gets all files needed by MSGF
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim eResult As IJobParams.CloseOutType

		m_PendingFileRenames = New Dictionary(Of String, String)

		' Make sure the machine has enough free memory to run MSGF
		If Not ValidateFreeMemorySize("MSGFJavaMemorySize", "MSGF") Then
			m_message = "Not enough free memory to run MSGF"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Get analysis results files
		eResult = GetInputFiles(m_jobParams.GetParam("ResultType"))
		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Retrieves input files needed for MSGF
	''' </summary>
	''' <param name="ResultType">String specifying type of analysis results input to extraction process</param>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Private Function GetInputFiles(ByVal ResultType As String) As IJobParams.CloseOutType

		Dim eResultType As clsPHRPReader.ePeptideHitResultType

		Dim RawDataType As String
		Dim eRawDataType As eRawDataTypeConstants
		Dim blnMGFInstrumentData As Boolean

		Dim FileToGet As String
		Dim strMzXMLFilePath As String = String.Empty
		Dim strSynFilePath As String = String.Empty

		Dim blnSuccess As Boolean = False
		Dim blnOnlyCopyFHTandSYNfiles As Boolean

		' Make sure the ResultType is valid
		eResultType = clsPHRPReader.GetPeptideHitResultType(ResultType)

		If eResultType = clsPHRPReader.ePeptideHitResultType.Sequest OrElse
		  eResultType = clsPHRPReader.ePeptideHitResultType.XTandem OrElse
		  eResultType = clsPHRPReader.ePeptideHitResultType.Inspect OrElse
		  eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB OrElse
		  eResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
			blnSuccess = True
		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Invalid tool result type (not supported by MSGF): " & ResultType)
			blnSuccess = False
		End If

		If Not blnSuccess Then
			Return (IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES)
		End If

		' Make sure the dataset type is valid
		RawDataType = m_jobParams.GetParam("RawDataType")
		eRawDataType = GetRawDataType(RawDataType)
		blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", False)

		If eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
			' We do not need the mzXML file, the parameter file, or various other files if we are running MSGFDB and running MSGF v6432 or later
			' Determine this by looking for job parameter MSGF_Version

			Dim strMSGFStepToolVersion As String = m_jobParams.GetParam("MSGF_Version")

			If String.IsNullOrWhiteSpace(strMSGFStepToolVersion) Then
				' Production version of MSGFDB; don't need the parameter file, ModSummary file, or mzXML file
				blnOnlyCopyFHTandSYNfiles = True
			Else
				' Specific version of MSGF is defined
				' Check whether the version is one of the known versions for the old MSGF
				If clsMSGFRunner.IsLegacyMSGFVersion(strMSGFStepToolVersion) Then
					blnOnlyCopyFHTandSYNfiles = False
				Else
					blnOnlyCopyFHTandSYNfiles = True
				End If
			End If
		ElseIf eResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
			' We do not need any raw data files for MODa
			blnOnlyCopyFHTandSYNfiles = True

		Else
			' Not running MSGFDB or running MSFDB but using legacy msgf
			blnOnlyCopyFHTandSYNfiles = False

			If Not blnMGFInstrumentData Then
				Select Case eRawDataType
					Case eRawDataTypeConstants.ThermoRawFile, eRawDataTypeConstants.mzML, eRawDataTypeConstants.mzXML
						' This is a valid data type
					Case Else
						m_message = "Dataset type " & RawDataType & " is not supported by MSGF"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message & "; must be one of the following: " & RAW_DATA_TYPE_DOT_RAW_FILES & ", " & RAW_DATA_TYPE_DOT_MZML_FILES & ", " & RAW_DATA_TYPE_DOT_MZXML_FILES)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Select
			End If

		End If

		If Not blnOnlyCopyFHTandSYNfiles Then
			' Get the Sequest, X!Tandem, Inspect, MSGF+, or MODa parameter file
			FileToGet = m_jobParams.GetParam("ParmFileName")
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)

			' Also copy the _ProteinMods.txt file
			FileToGet = clsPHRPReader.GetPHRPProteinModsFileName(eResultType, m_DatasetName)
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				' Ignore this error; we don't really need this file
			Else
				m_jobParams.AddResultFileToKeep(FileToGet)
			End If

		End If

		' Get the Sequest, X!Tandem, Inspect, MSGF+, or MODa PHRP _syn.txt file
		FileToGet = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
			strSynFilePath = Path.Combine(m_WorkingDir, FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF+ PHRP _fht.txt file
		FileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF+ PHRP _ResultToSeqMap.txt file
		FileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF+ PHRP _SeqToProteinMap.txt file
		FileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF+ PHRP _PepToProtMapMTS.txt file
		FileToGet = clsPHRPReader.GetPHRPPepToProteinMapFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Dim SynFileSizeBytes As Int64 = 0
		Dim ioSynFile As FileInfo = New FileInfo(strSynFilePath)
		If ioSynFile.Exists Then
			SynFileSizeBytes = ioSynFile.Length
		End If

		If Not blnOnlyCopyFHTandSYNfiles Then
			' Get the ModSummary.txt file        
			FileToGet = clsPHRPReader.GetPHRPModSummaryFileName(eResultType, m_DatasetName)
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				' _ModSummary.txt file not found
				' This will happen if the synopsis file is empty
				' Try to copy the _ModDefs.txt file instead

				If SynFileSizeBytes = 0 Then
					' If the synopsis file is 0-bytes, then the _ModSummary.txt file won't exist; that's OK
					Dim strModDefsFile As String
					Dim strTargetFile As String = Path.Combine(m_WorkingDir, FileToGet)

					strModDefsFile = Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) & PHRP_MOD_DEFS_SUFFIX

					If Not FindAndRetrieveMiscFiles(strModDefsFile, False) Then
						' Rename the file to end in _ModSummary.txt
						m_PendingFileRenames.Add(strModDefsFile, strTargetFile)
					Else
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
					End If
				Else
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
				End If
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
		End If

		' Copy the PHRP files so that the PHRPReader can determine the modified residues and extract the protein names
		' clsMSGFResultsSummarizer also uses these files

		FileToGet = clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If FindAndRetrieveMiscFiles(FileToGet, False) Then
				m_jobParams.AddResultFileToSkip(FileToGet)
			Else
				If SynFileSizeBytes = 0 Then
					' If the synopsis file is 0-bytes, then the _ResultToSeqMap.txt file won't exist
					' That's OK; we'll create an empty file with just a header line
					If Not CreateEmptyResultToSeqMapFile(FileToGet) Then
						Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
					End If
				Else
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
				End If
			End If

		End If

		FileToGet = clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If FindAndRetrieveMiscFiles(FileToGet, False) Then
				m_jobParams.AddResultFileToSkip(FileToGet)
			Else
				If SynFileSizeBytes = 0 Then
					' If the synopsis file is 0-bytes, then the _SeqToProteinMap.txt file won't exist
					' That's OK; we'll create an empty file with just a header line
					If Not CreateEmptySeqToProteinMapFile(FileToGet) Then
						Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
					End If
				Else
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
				End If
			End If
		End If

		FileToGet = clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, m_DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If FindAndRetrieveMiscFiles(FileToGet, False) Then
				m_jobParams.AddResultFileToSkip(FileToGet)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "SeqInfo file not found (" & FileToGet & "); modifications will be inferred using the ModSummary.txt file")
			End If
		End If

		If blnMGFInstrumentData Then

			Dim strFileToFind As String = m_DatasetName & DOT_MGF_EXTENSION
			If Not FindAndRetrieveMiscFiles(strFileToFind, False) Then
				m_message = "Instrument data not found: " & strFileToFind
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesMSGF.GetResources: " & m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				m_jobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION)
			End If

		ElseIf Not blnOnlyCopyFHTandSYNfiles Then

			' See if a .mzXML file already exists for this dataset
			blnSuccess = RetrieveMZXmlFile(False, strMzXMLFilePath)

			' Make sure we don't move the .mzXML file into the results folder
			m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)

			If blnSuccess Then
				' .mzXML file found and copied locally; no need to retrieve the .Raw file
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Existing .mzXML file found: " & strMzXMLFilePath)
				End If

				' Possibly unzip the .mzXML file
				Dim fiMzXMLFile = New FileInfo(Path.Combine(m_WorkingDir & DOT_MZXML_EXTENSION & DOT_GZ_EXTENSION))
				If fiMzXMLFile.Exists Then
					m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION)

					If Not m_IonicZipTools.GUnzipFile(fiMzXMLFile.FullName) Then
						m_message = "Error decompressing .mzXML.gz file"
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If

				End If
			Else
				' .mzXML file not found
				' Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
				If RetrieveSpectra(RawDataType) Then
					m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)			' Raw file
					m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)		' mzXML file
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			End If

		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		For Each entry In m_PendingFileRenames
			Dim sourceFile As New FileInfo(Path.Combine(m_WorkingDir, entry.Key))
			If sourceFile.Exists Then
				sourceFile.MoveTo(Path.Combine(m_WorkingDir, entry.Value))
			End If
		Next

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function CreateEmptyResultToSeqMapFile(ByVal FileName As String) As Boolean
		Dim strFilePath As String

		Try
			strFilePath = Path.Combine(m_WorkingDir, FileName)
			Using swOutfile As StreamWriter = New StreamWriter(New FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
				swOutfile.WriteLine("Result_ID" & ControlChars.Tab & "Unique_Seq_ID")
			End Using
		Catch ex As Exception
			Dim Msg As String = "Error creating empty ResultToSeqMap file: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return False
		End Try

		Return True
	End Function

	Private Function CreateEmptySeqToProteinMapFile(ByVal FileName As String) As Boolean
		Dim strFilePath As String

		Try
			strFilePath = Path.Combine(m_WorkingDir, FileName)
			Using swOutfile As StreamWriter = New StreamWriter(New FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
				swOutfile.WriteLine("Unique_Seq_ID" & ControlChars.Tab & "Cleavage_State" & ControlChars.Tab & "Terminus_State" & ControlChars.Tab & "Protein_Name" & ControlChars.Tab & "Protein_Expectation_Value_Log(e)" & ControlChars.Tab & "Protein_Intensity_Log(I)")
			End Using
		Catch ex As Exception
			Dim Msg As String = "Error creating empty SeqToProteinMap file: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return False
		End Try

		Return True

	End Function
#End Region

End Class
