'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGF
	Inherits clsAnalysisResources

	'*********************************************************************************************************
    'Manages retrieval of all files needed by MSGF
	'*********************************************************************************************************

#Region "Constants"
#End Region

#Region "Module variables"
#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
    ''' Gets all files needed by MSGF
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResult As IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

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
	Private Function GetInputFiles(ByVal ResultType As String) As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResultType As clsMSGFRunner.ePeptideHitResultType

        Dim DatasetName As String
        Dim RawDataType As String

		Dim FileToGet As String
		Dim SynFileSizeBytes As Int64
        Dim strMzXMLFilePath As String = String.Empty

        Dim blnSuccess As Boolean = False
		Dim blnOnlyCopyFHTandSYNfiles As Boolean

        ' Cache the dataset name
        DatasetName = m_jobParams.GetParam("DatasetNum")

        ' Make sure the ResultType is valid
        eResultType = clsMSGFRunner.GetPeptideHitResultType(ResultType)

        If eResultType = clsMSGFRunner.ePeptideHitResultType.Sequest OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.XTandem OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.Inspect OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.MSGFDB Then
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

		If eResultType = clsMSGFRunner.ePeptideHitResultType.MSGFDB Then
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

		Else
			' Not running MSGFDB or running MSFDB but using legacy msgf
			blnOnlyCopyFHTandSYNfiles = False

			If RawDataType.ToLower <> RAW_DATA_TYPE_DOT_RAW_FILES Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMSGF.GetResources: Dataset type " & RawDataType & " is not supported; must be " & RAW_DATA_TYPE_DOT_RAW_FILES)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		End If

		If Not blnOnlyCopyFHTandSYNfiles Then
			' Get the Sequest, X!Tandem, Inspect, or MSGF-DB parameter file
			FileToGet = m_jobParams.GetParam("ParmFileName")
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF-DB PHRP _syn.txt file
		FileToGet = clsMSGFRunner.GetPHRPSynopsisFileName(eResultType, DatasetName)
		SynFileSizeBytes = 0
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)

			Dim ioSynFile As System.IO.FileInfo = New System.IO.FileInfo(FileToGet)
			If ioSynFile.Exists Then
				SynFileSizeBytes = ioSynFile.Length
			End If
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF-DB PHRP _fht.txt file
		FileToGet = clsMSGFRunner.GetPHRPFirstHitsFileName(eResultType, DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF-DB PHRP _ResultToSeqMap.txt file
		FileToGet = clsMSGFRunner.GetPHRPFirstHitsFileName(eResultType, DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If

		' Get the Sequest, X!Tandem, Inspect, or MSGF-DB PHRP _SeqToProteinMap.txt file
		FileToGet = clsMSGFRunner.GetPHRPFirstHitsFileName(eResultType, DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If

		If Not blnOnlyCopyFHTandSYNfiles Then
			' Get the ModSummary.txt file        
			FileToGet = clsMSGFRunner.GetModSummaryFileName(eResultType, DatasetName)
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				' _ModSummary.txt file not found
				' This will happen if the synopsis file is empty
				' Try to copy the _ModDefs.txt file instead

				If SynFileSizeBytes = 0 Then
					' If the synopsis file is 0-bytes, then the _ModSummary.txt file won't exist; that's OK
					Dim strModDefsFile As String
					Dim strTargetFile As String = System.IO.Path.Combine(m_WorkingDir, FileToGet)

					strModDefsFile = System.IO.Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) & clsMSGFRunner.PHRP_MOD_DEFS_SUFFIX

					If FindAndRetrieveMiscFiles(strModDefsFile, False) Then
						' Rename the file to end in _ModSummary.txt
						strModDefsFile = System.IO.Path.Combine(m_WorkingDir, strModDefsFile)

						System.IO.File.Copy(strModDefsFile, strTargetFile, True)
						System.Threading.Thread.Sleep(100)
						System.IO.File.Delete(strModDefsFile)
					Else
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
					End If
				Else
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
				End If
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If


		' Copy the PHRP files so that we can extract the protein names
		' This information is added the MSGF files for X!Tandem
		' It is used by clsMSGFResultsSummarizer for all tools

		FileToGet = clsMSGFRunner.GetPHRPResultToSeqMapFileName(eResultType, DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If

		FileToGet = clsMSGFRunner.GetPHRPSeqToProteinMapFileName(eResultType, DatasetName)
		If Not String.IsNullOrEmpty(FileToGet) Then
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
			clsGlobal.FilesToDelete.Add(FileToGet)
		End If

		If Not blnOnlyCopyFHTandSYNfiles Then

			' See if a .mzXML file already exists for this dataset
			blnSuccess = RetrieveMZXmlFile(m_WorkingDir, False, strMzXMLFilePath)

			' Make sure we don't move the .mzXML file into the results folder
			clsGlobal.m_FilesToDeleteExt.Add(".mzXML")

			If blnSuccess Then
				' .mzXML file found and copied locally; no need to retrieve the .Raw file
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Existing .mzXML file found: " & strMzXMLFilePath)
				End If
			Else
				' .mzXML file not found
				' Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
				If RetrieveSpectra(RawDataType, m_WorkingDir) Then
					clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_RAW_EXTENSION)			' Raw file
					clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_MZXML_EXTENSION)			' mzXML file
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			End If

		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#End Region

End Class
