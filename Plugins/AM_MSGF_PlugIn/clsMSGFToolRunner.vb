'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports AnalysisManagerMsXmlGenPlugIn
Imports PHRPReader

Public Class clsMSGFRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Primary class for running MSGF
	'*********************************************************************************************************

#Region "Constants and enums"

	Protected Const PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD As Single = 2
	Protected Const PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED As Single = 3
	Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 6
	Protected Const PROGRESS_PCT_MZXML_CREATED As Single = 10
	Protected Const PROGRESS_PCT_MSGF_START As Single = PROGRESS_PCT_MZXML_CREATED
	Protected Const PROGRESS_PCT_MSGF_COMPLETE As Single = 95
	Protected Const PROGRESS_PCT_MSGF_POST_PROCESSING As Single = 97

	Public Const MSGF_RESULT_COLUMN_SpectrumFile As String = "#SpectrumFile"
	Public Const MSGF_RESULT_COLUMN_Title As String = "Title"
	Public Const MSGF_RESULT_COLUMN_ScanNumber As String = "Scan#"
	Public Const MSGF_RESULT_COLUMN_Annotation As String = "Annotation"
	Public Const MSGF_RESULT_COLUMN_Charge As String = "Charge"
	Public Const MSGF_RESULT_COLUMN_Protein_First As String = "Protein_First"
	Public Const MSGF_RESULT_COLUMN_Result_ID As String = "Result_ID"
	Public Const MSGF_RESULT_COLUMN_SpecProb As String = "SpecProb"
	Public Const MSGF_RESULT_COLUMN_Data_Source As String = "Data_Source"
	Public Const MSGF_RESULT_COLUMN_Collision_Mode As String = "Collision_Mode"

	Public Const MSGF_PHRP_DATA_SOURCE_SYN As String = "Syn"
	Public Const MSGF_PHRP_DATA_SOURCE_FHT As String = "FHT"

	Public Const MSGF_SEGMENT_ENTRY_COUNT As Integer = 25000
	Public Const MSGF_SEGMENT_OVERFLOW_MARGIN As Single = 0.05			' If the final segment is less than 5% of MSGF_SEGMENT_ENTRY_COUNT then combine the data with the previous segment

	Protected Const MSGF_CONSOLE_OUTPUT As String = "MSGF_ConsoleOutput.txt"
	Protected Const MSGF_JAR_NAME As String = "MSGF.jar"
	Protected Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"


	Protected Structure udtSegmentFileInfoType
		Public Segment As Integer		' Segment number
		Public FilePath As String		' Full path to the file
		Public Entries As Integer		' Number of entries in this segment
	End Structure

#End Region

#Region "Module variables"
	Protected mETDMode As Boolean = False

	Protected mMSGFInputFilePath As String = String.Empty
	Protected mMSGFResultsFilePath As String = String.Empty
	Protected mCurrentMSGFResultsFilePath As String = String.Empty

	Protected mMSGFInputFileLineCount As Integer = 0
	Protected mMSGFLineCountPreviousSegments As Integer = 0

	Protected mProcessingMSGFDBCollisionModeData As Boolean
	Protected mCollisionModeIteration As Integer

	Protected mKeepMSGFInputFiles As Boolean = False

	Protected mToolVersionWritten As Boolean
	Protected mMSGFVersion As String = String.Empty
	Protected mMSGFProgLoc As String = String.Empty

	Protected mMSXmlGeneratorExe As String = String.Empty			' ReadW.exe or MSConvert.exe (code will assume ReadW.exe if an empty string)
	Protected mMSXmlGeneratorAppPath As String = String.Empty

	Protected mUsingMSGFDB As Boolean = True
	Protected mMSGFDBVersion As String = "Unknown"

	Protected mJavaProgLoc As String = String.Empty

	Protected mConsoleOutputErrorMsg As String

	Protected WithEvents mMSXmlGen As clsMSXmlGen
	Protected WithEvents mMSGFInputCreator As clsMSGFInputCreator
	Protected WithEvents mMSGFRunner As clsRunDosProgram

#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSGF
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim eResultType As clsPHRPReader.ePeptideHitResultType
		Dim Msg As String = String.Empty

		Dim blnSuccess As Boolean
		Dim Result As IJobParams.CloseOutType
		Dim eReturnCode As IJobParams.CloseOutType

		Dim blnProcessingError As Boolean
		Dim blnUseExistingMSGFResults As Boolean
		Dim blnPostProcessingError As Boolean

		Dim blnDoNotFilterPeptides As Boolean
		Dim intMSGFInputFileLineCount As Integer = 0

		' Set this to success for now
		eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		'Call base class for initial setup
		If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Resolve eResultType
		eResultType = GetPeptideHitResultType(m_jobParams.GetParam("ResultType"))

		If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
			' Result type is not supported

			Msg = "ResultType is not supported by MSGF: " & m_jobParams.GetParam("ResultType")
			m_message = clsGlobal.AppendToComment(m_message, Msg)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMSGFToolRunner.RunTool(); " & Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Verify that program files exist
		If Not DefineProgramPaths() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Note: we will store the MSGF version info in the database after the first line is written to file MSGF_ConsoleOutput.txt
		mToolVersionWritten = False
		mMSGFVersion = String.Empty
		mConsoleOutputErrorMsg = String.Empty

		mKeepMSGFInputFiles = m_JobParams.GetJobParameter("KeepMSGFInputFile", False)
		blnDoNotFilterPeptides = m_JobParams.GetJobParameter("MSGFIgnoreFilters", False)

		Try
			blnProcessingError = False


			If mUsingMSGFDB And eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
				' Don't actually run MSGF
				' Simply copy the values from the MSGFDB result file

				StoreToolVersionInfoMSGFDBResults()

				If Not CreateMSGFResultsFromMSGFDBResults() Then
					blnProcessingError = True
				End If

			Else

				' Parse the Sequest, X!Tandem, or Inspect parameter file to determine if ETD mode was used
				Dim strSearchToolParamFilePath As String
				strSearchToolParamFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"))

				blnSuccess = CheckETDModeEnabled(eResultType, strSearchToolParamFilePath)
				If Not blnSuccess Then
					Msg = "Error examining param file to determine if ETD mode was enabled)"
					m_message = clsGlobal.AppendToComment(m_message, Msg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMSGFToolRunner.RunTool(); " & Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				Else
					m_progress = PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD
					m_StatusTools.UpdateAndWrite(m_progress)
				End If

				' Create the _MSGF_input.txt file
				blnSuccess = CreateMSGFInputFile(eResultType, blnDoNotFilterPeptides, intMSGFInputFileLineCount)

				If Not blnSuccess Then
					Msg = "Error creating MSGF input file"
					m_message = clsGlobal.AppendToComment(m_message, Msg)
					blnProcessingError = True
				Else
					m_progress = PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED
					m_StatusTools.UpdateAndWrite(m_progress)
				End If


				If Not blnProcessingError Then
					' Create the .mzXML file
					' We're waiting to do this until now just in case the above steps fail (since they should all run quickly)
					blnSuccess = CreateMZXMLFile(m_WorkDir)

					If Not blnSuccess Then
						Msg = "Error creating .mzXML file"
						m_message = clsGlobal.AppendToComment(m_message, Msg)
						blnProcessingError = True
					Else
						m_progress = PROGRESS_PCT_MZXML_CREATED
						m_StatusTools.UpdateAndWrite(m_progress)
					End If
				End If


				If Not blnProcessingError Then
					blnUseExistingMSGFResults = m_JobParams.GetJobParameter("UseExistingMSGFResults", False)

					If blnUseExistingMSGFResults Then
						' Look for a file named Dataset_syn_MSGF.txt in the job's transfer folder
						' If that file exists, use it as the official MSGF results file
						' The assumption is that this file will have been created by manually running MSGF on another computer

						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "UseExistingMSGFResults = True; will look for pre-generated MSGF results file in the transfer folder")
						End If

						If RetrievePreGeneratedDataFile(System.IO.Path.GetFileName(mMSGFResultsFilePath)) Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Pre-generated MSGF results file successfully copied to the work directory")
							blnSuccess = True
						Else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Pre-generated MSGF results file not found")
							blnSuccess = False
						End If

					Else
						' Run MSGF
						' Note that mMSGFInputFilePath and mMSGFResultsFilePath get populated by CreateMSGFInputFile
						blnSuccess = ProcessFileWithMSGF(eResultType, intMSGFInputFileLineCount, mMSGFInputFilePath, mMSGFResultsFilePath)
					End If

					If Not blnSuccess Then
						Msg = "Error running MSGF"
						m_message = clsGlobal.AppendToComment(m_message, Msg)
						blnProcessingError = True
					Else
						' MSGF successfully completed
						If Not mKeepMSGFInputFiles Then
							' Add the _MSGF_input.txt file to the list of files to delete (i.e., do not move it into the results folder)
							m_jobParams.AddResultFileToSkip(System.IO.Path.GetFileName(mMSGFInputFilePath))
						End If

						m_progress = PROGRESS_PCT_MSGF_COMPLETE
						m_StatusTools.UpdateAndWrite(m_progress)
					End If
				End If

				If Not blnProcessingError Then
					' Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
					' Will also make sure that all of the peptides have numeric SpecProb values
					' For peptides where MSGF reported an error, the MSGF SpecProb will be set to 1

					' Sleep for 1 second to give the MSGF results file a chance to finalize
					System.Threading.Thread.Sleep(1000)

					blnSuccess = PostProcessMSGFResults(eResultType, mMSGFResultsFilePath)

					If Not blnSuccess Then
						Msg = "MSGF results file post-processing error"
						m_message = clsGlobal.AppendToComment(m_message, Msg)
						blnPostProcessingError = True
					End If

				End If

				' Make sure the MSGF Input Creator log file is closed
				mMSGFInputCreator.CloseLogFileNow()

			End If

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			If blnProcessingError Then
				' Something went wrong
				' In order to help diagnose things, we will move whatever files were created into the result folder, 
				'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			System.Threading.Thread.Sleep(2000)		   '2 second delay
			GC.Collect()
			GC.WaitForPendingFinalizers()

			Result = MakeResultsFolder()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'MakeResultsFolder handles posting to local log, so set database error message and exit
				m_message = "Error making results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Result = MoveResultFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'MoveResultFiles moves the result files to the result folder
				m_message = "Error moving files into results folder"
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
				' Try to save whatever files were moved into the results folder
				Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
				objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Result = CopyResultsFolderToServer()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				Return Result
			End If

			If blnPostProcessingError Then
				' When a post-processing error occurs, we copy the files to the server, but return CLOSEOUT_FAILED
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As System.Exception
			Msg = "clsMSGFToolRunner.RunTool(); Exception running MSGF: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running MSGF")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'If we get to here, everything worked so exit happily
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function AddFileNameSuffix(ByVal strFilePath As String, ByVal intSuffix As Integer) As String
		Return AddFileNameSuffix(strFilePath, intSuffix.ToString())
	End Function

	Protected Function AddFileNameSuffix(ByVal strFilePath As String, ByVal strSuffix As String) As String
		Dim fiFile As System.IO.FileInfo
		Dim strFilePathNew As String

		fiFile = New System.IO.FileInfo(strFilePath)
		strFilePathNew = System.IO.Path.Combine(fiFile.DirectoryName, System.IO.Path.GetFileNameWithoutExtension(fiFile.Name) & "_" & strSuffix & fiFile.Extension)

		Return strFilePathNew

	End Function

	''' <summary>
	''' Examines the Sequest, X!Tandem, Inspect, or MSGFDB param file to determine if ETD mode is enabled
	''' </summary>
	''' <param name="eResultType"></param>
	''' <param name="strSearchToolParamFilePath"></param>
	''' <returns>True if success; false if an error</returns>
	Protected Function CheckETDModeEnabled(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, ByVal strSearchToolParamFilePath As String) As Boolean

		Dim blnSuccess As Boolean

		mETDMode = False
		blnSuccess = False

		If String.IsNullOrEmpty(strSearchToolParamFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PeptideHit param file path is empty; unable to continue")
			Return False
		End If

		m_StatusTools.CurrentOperation = "Checking whether ETD mode is enabled"

		Select Case eResultType
			Case clsPHRPReader.ePeptideHitResultType.Sequest
				blnSuccess = CheckETDModeEnabledSequest(strSearchToolParamFilePath)

			Case clsPHRPReader.ePeptideHitResultType.XTandem
				blnSuccess = CheckETDModeEnabledXTandem(strSearchToolParamFilePath)

			Case clsPHRPReader.ePeptideHitResultType.Inspect
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inspect does not support ETD data processing; will set mETDMode to False")
				blnSuccess = True

			Case clsPHRPReader.ePeptideHitResultType.MSGFDB
				blnSuccess = CheckETDModeEnabledMSGFDB(strSearchToolParamFilePath)

			Case Else
				' Unknown result type
		End Select

		If mETDMode Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ETD search mode has been enabled since c and z ions were used for the peptide search")
		End If

		Return blnSuccess

	End Function

	Protected Function CheckETDModeEnabledMSGFDB(ByVal strSearchToolParamFilePath As String) As Boolean


		Const MSGFDB_FRAG_METHOD_TAG As String = "FragmentationMethodID"

		Dim strLineIn As String

		Dim strFragMode As String
		Dim intFragMode As Integer

		Dim intLinesRead As Integer
		Dim intCharIndex As Integer

		Try
			mETDMode = False

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Reading the MSGF-DB parameter file: " & strSearchToolParamFilePath)
			End If

			' Read the data from the MSGF-DB Param file
			Using srParamFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strSearchToolParamFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

				intLinesRead = 0

				Do While srParamFile.Peek >= 0
					strLineIn = srParamFile.ReadLine
					intLinesRead += 1

					If Not String.IsNullOrEmpty(strLineIn) AndAlso _
					   strLineIn.StartsWith(MSGFDB_FRAG_METHOD_TAG) Then

						' Check whether this line is FragmentationMethodID=2
						' Note that FragmentationMethodID=4 means Merge spectra from the same precursor (e.g. CID/ETD pairs, CID/HCD/ETD triplets)  
						' This mode is not yet supported

						If m_DebugLevel >= 3 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB " & MSGFDB_FRAG_METHOD_TAG & " line found: " & strLineIn)
						End If

						' Look for the equals sign
						intCharIndex = strLineIn.IndexOf("=")
						If intCharIndex > 0 Then
							strFragMode = strLineIn.Substring(intCharIndex + 1).Trim

							If Integer.TryParse(strFragMode, intFragMode) Then
								If intFragMode = 2 Then
									mETDMode = True
								ElseIf intFragMode = 4 Then
									' ToDo: Figure out how to handle this mode
									mETDMode = False
								Else
									mETDMode = False
								End If
							End If

						Else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGFDB " & MSGFDB_FRAG_METHOD_TAG & " line does not have an equals sign; will assume not using ETD ions: " & strLineIn)
						End If

						' No point in checking any further since we've parsed the ion_series line
						Exit Do

					End If

				Loop


			End Using


		Catch ex As Exception
			Dim Msg As String
			Msg = "Error reading the MSGFDB param file: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception reading MSGFDB parameter file")

			Return False
		End Try

		Return True


	End Function


	''' <summary>
	''' Examines the Sequest param file to determine if ETD mode is enabled
	''' If it is, then sets mETDMode to True
	''' </summary>
	''' <param name="strSearchToolParamFilePath">Sequest parameter file to read</param>
	''' <returns>True if success; false if an error</returns>
	Protected Function CheckETDModeEnabledSequest(ByVal strSearchToolParamFilePath As String) As Boolean

		Const SEQUEST_ION_SERIES_TAG As String = "ion_series"

		Dim strLineIn As String

		Dim strIonWeightText As String
		Dim strIonWeights() As String

		Dim dblCWeight As Double
		Dim dblZWeight As Double

		Dim strTag As String = String.Empty
		Dim strSetting As String = String.Empty

		Dim intLinesRead As Integer
		Dim intCharIndex As Integer

		Try
			mETDMode = False

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Reading the Sequest parameter file: " & strSearchToolParamFilePath)
			End If

			' Read the data from the Sequest Param file
			Using srParamFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strSearchToolParamFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

				intLinesRead = 0

				Do While srParamFile.Peek >= 0
					strLineIn = srParamFile.ReadLine
					intLinesRead += 1

					If Not String.IsNullOrEmpty(strLineIn) AndAlso _
					   strLineIn.StartsWith(SEQUEST_ION_SERIES_TAG) Then

						' This is the ion_series line
						' If ETD mode is enabled, then c and z ions will have a 1 in this series of numbers:
						' ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0 
						'
						' The key to parsing this data is:
						' ion_series = - - -  a   b   c  --- --- ---  x   y   z
						' ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0 

						If m_DebugLevel >= 3 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest " & SEQUEST_ION_SERIES_TAG & " line found: " & strLineIn)
						End If

						' Look for the equals sign
						intCharIndex = strLineIn.IndexOf("=")
						If intCharIndex > 0 Then
							strIonWeightText = strLineIn.Substring(intCharIndex + 1).Trim

							' Split strIonWeightText on spaces
							strIonWeights = strIonWeightText.Split(" "c)

							If strIonWeights.Length >= 12 Then
								dblCWeight = 0
								dblZWeight = 0

								Double.TryParse(strIonWeights(5), dblCWeight)
								Double.TryParse(strIonWeights(11), dblZWeight)

								If m_DebugLevel >= 3 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest " & SEQUEST_ION_SERIES_TAG & " line has c-ion weighting = " & dblCWeight & " and z-ion weighting = " & dblZWeight)
								End If

								If dblCWeight > 0 OrElse dblZWeight > 0 Then
									mETDMode = True
								End If
							Else
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest " & SEQUEST_ION_SERIES_TAG & " line does not have 11 numbers; will assume not using ETD ions: " & strLineIn)
							End If
						Else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest " & SEQUEST_ION_SERIES_TAG & " line does not have an equals sign; will assume not using ETD ions: " & strLineIn)
						End If

						' No point in checking any further since we've parsed the ion_series line
						Exit Do

					End If

				Loop

			End Using


		Catch ex As Exception
			Dim Msg As String
			Msg = "Error reading the Sequest param file: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception reading Sequest parameter file")
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Examines the X!Tndem param file to determine if ETD mode is enabled
	''' If it is, then sets mETDMode to True
	''' </summary>
	''' <param name="strSearchToolParamFilePath">X!Tandem XML parameter file to read</param>
	''' <returns>True if success; false if an error</returns>
	Protected Function CheckETDModeEnabledXTandem(ByVal strSearchToolParamFilePath As String) As Boolean

		Dim objParamFile As System.Xml.XmlDocument

		Dim objSelectedNodes As System.Xml.XmlNodeList = Nothing
		Dim objAttributeNode As System.Xml.XmlNode

		Dim intSettingIndex As Integer
		Dim intMatchIndex As Integer

		Try
			mETDMode = False

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Reading the X!Tandem parameter file: " & strSearchToolParamFilePath)
			End If

			' Open the parameter file
			' Look for either of these lines:
			'   <note type="input" label="scoring, c ions">yes</note>
			'   <note type="input" label="scoring, z ions">yes</note>

			objParamFile = New System.Xml.XmlDocument
			objParamFile.PreserveWhitespace = True
			objParamFile.Load(strSearchToolParamFilePath)

			For intSettingIndex = 0 To 1
				Select Case intSettingIndex
					Case 0
						objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, c ions']")
					Case 1
						objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, z ions']")
				End Select

				If Not objSelectedNodes Is Nothing Then

					For intMatchIndex = 0 To objSelectedNodes.Count - 1
						' Make sure this node has an attribute named type with value "input"
						objAttributeNode = objSelectedNodes.Item(intMatchIndex).Attributes.GetNamedItem("type")

						If objAttributeNode Is Nothing Then
							' Node does not have an attribute named "type"
						Else
							If objAttributeNode.Value.ToLower = "input" Then
								' Valid node; examine its InnerText value
								If objSelectedNodes.Item(intMatchIndex).InnerText.ToLower() = "yes" Then
									mETDMode = True
								End If
							End If
						End If
					Next intMatchIndex

				End If

				If mETDMode Then Exit For
			Next intSettingIndex

		Catch ex As Exception

			Dim Msg As String
			Msg = "Error reading the X!Tandem param file: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception reading X!Tandem parameter file")

			Return False
		End Try

		Return True
	End Function

	''' <summary>
	''' Creates the MSGF Input file by reading Sequest, X!Tandem, or Inspect PHRP result file and extracting the relevant information
	''' Uses the ModSummary.txt file to determine the dynamic and static mods used
	''' </summary>
	''' <param name="eResultType"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function CreateMSGFInputFile(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, _
	 ByVal blnDoNotFilterPeptides As Boolean, _
	 ByRef intMSGFInputFileLineCount As Integer) As Boolean

		Dim Msg As String

		Dim blnSuccess As Boolean = True

		intMSGFInputFileLineCount = 0

		' Convert the peptide-hit result file (from PHRP) to a tab-delimited input file to be read by MSGF
		Select Case eResultType
			Case clsPHRPReader.ePeptideHitResultType.Sequest

				' Convert Sequest results to input format required for MSGF
				mMSGFInputCreator = New clsMSGFInputCreatorSequest(m_Dataset, m_WorkDir)

			Case clsPHRPReader.ePeptideHitResultType.XTandem

				' Convert X!Tandem results to input format required for MSGF
				mMSGFInputCreator = New clsMSGFInputCreatorXTandem(m_Dataset, m_WorkDir)


			Case clsPHRPReader.ePeptideHitResultType.Inspect

				' Convert Inspect results to input format required for MSGF
				mMSGFInputCreator = New clsMSGFInputCreatorInspect(m_Dataset, m_WorkDir)

			Case clsPHRPReader.ePeptideHitResultType.MSGFDB

				' Convert MSGFDB results to input format required for MSGF
				mMSGFInputCreator = New clsMSGFInputCreatorMSGFDB(m_Dataset, m_WorkDir)

			Case Else
				' Should never get here; invalid result type specified
				Msg = "Invalid PeptideHit ResultType specified: " & eResultType
				m_message = clsGlobal.AppendToComment(m_message, Msg)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMSGFToolRunner.CreateMSGFInputFile(); " & Msg)

				blnSuccess = False
		End Select

		If blnSuccess Then

			mMSGFInputFilePath = mMSGFInputCreator.MSGFInputFilePath()
			mMSGFResultsFilePath = mMSGFInputCreator.MSGFResultsFilePath()

			mMSGFInputCreator.DoNotFilterPeptides = blnDoNotFilterPeptides

			m_StatusTools.CurrentOperation = "Creating the MSGF Input file"

			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating the MSGF Input file")
			End If

			blnSuccess = mMSGFInputCreator.CreateMSGFInputFileUsingPHRPResultFiles()

			intMSGFInputFileLineCount = mMSGFInputCreator.MSGFInputFileLineCount

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mMSGFInputCreator.MSGFDataFileLineCount returned False")
			Else
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CreateMSGFInputFileUsingPHRPResultFile complete; " & intMSGFInputFileLineCount & " lines of data")
				End If
			End If

		End If


		Return blnSuccess

	End Function


	Private Function CreateMSGFResultsFromMSGFDBResults() As Boolean

		Dim objMSGFInputCreator As New clsMSGFInputCreatorMSGFDB(m_Dataset, m_WorkDir)
		Dim blnSuccess As Boolean

		If Not CreateMSGFResultsFromMSGFDBResults(objMSGFInputCreator, MSGF_PHRP_DATA_SOURCE_SYN.ToLower()) Then
			Return False
		End If

		If Not CreateMSGFResultsFromMSGFDBResults(objMSGFInputCreator, MSGF_PHRP_DATA_SOURCE_FHT.ToLower()) Then
			Return False
		End If

		' Summarize the results in the _syn_MSGF.txt file
		' Post the results to the database
		blnSuccess = SummarizeMSGFResults(clsPHRPReader.ePeptideHitResultType.MSGFDB)

		Return blnSuccess

	End Function

	Private Function CreateMSGFResultsFromMSGFDBResults(ByRef objMSGFInputCreator As clsMSGFInputCreatorMSGFDB, ByVal strSynOrFHT As String) As Boolean

		Dim strSourceFilePath As String
		Dim blnSuccess As Boolean

		strSourceFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_msgfdb_" & strSynOrFHT & ".txt")
		blnSuccess = objMSGFInputCreator.CreateMSGFFileUsingMSGFDBSpecProb(strSourceFilePath, strSynOrFHT)

		If Not blnSuccess Then
			m_message = "Error creating MSGF file for " & System.IO.Path.GetFileName(strSourceFilePath)
			If Not String.IsNullOrEmpty(objMSGFInputCreator.ErrorMessage) Then
				m_message &= ": " & objMSGFInputCreator.ErrorMessage
			End If
			Return False
		Else
			Return True
		End If

	End Function

	''' <summary>
	''' Generate the mzXML
	''' </summary>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Private Function CreateMZXMLFile(ByVal strInputFolderPath As String) As Boolean

		Dim dtStartTime As System.DateTime

		' Turn on Centroiding, which will result in faster mzXML file generation time and smaller .mzXML files
		Dim CentroidMSXML As Boolean = True

		Dim eOutputType As clsMSXmlGen.MSXMLOutputTypeConstants

		Dim blnSuccess As Boolean

		m_StatusTools.CurrentOperation = "Creating the .mzXML file"

		' mzXML filename is dataset plus .mzXML
		Dim strMzXmlFilePath As String
		strMzXmlFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)

		If System.IO.File.Exists(strMzXmlFilePath) Then
			' File already exists; nothing to do
			Return True
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating the .mzXML file for " & m_Dataset)
		End If

		eOutputType = clsMSXmlGen.MSXMLOutputTypeConstants.mzXML

		' Instantiate the processing class
		' Note that mMSXmlGeneratorExe and mMSXmlGeneratorAppPath should have been populated by StoreToolVersionInfo()
		' mMSXmlGeneratorExe comes from m_jobParams.GetParam("MSXMLGenerator")

		If mMSXmlGeneratorExe.ToLower().Contains("readw") Then
			' ReadW
			' mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

			mMSXmlGen = New clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eOutputType, CentroidMSXML)

		ElseIf mMSXmlGeneratorExe.ToLower().Contains("msconvert") Then
			' MSConvert

			' Lookup Centroid Settings
			CentroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", True)
			Dim CentroidPeakCountToRetain As Integer = m_jobParams.GetJobParameter("CentroidPeakCountToRetain", clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)

			mMSXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eOutputType, CentroidMSXML, CentroidPeakCountToRetain)

		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unsupported XmlGenerator: " & mMSXmlGeneratorExe)
			Return False
		End If


		dtStartTime = System.DateTime.UtcNow

		' Create the file
		blnSuccess = mMSXmlGen.CreateMSXMLFile()

		If Not blnSuccess Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mMSXmlGen.ErrorMessage)
			Return False

		ElseIf mMSXmlGen.ErrorMessage.Length > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mMSXmlGen.ErrorMessage)
		End If

		' Validate that the .mzXML file was actually created
		If Not System.IO.File.Exists(strMzXmlFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ".mzXML file was not created by ReadW: " & strMzXmlFilePath)
			Return False
		End If

		If m_DebugLevel >= 1 Then
			Try
				' Save some stats to the log

				Dim strMessage As String
				Dim ioFileInfo As System.IO.FileInfo
				Dim dblFileSizeMB As Double, dblXMLSizeMB As Double
				Dim dblTotalMinutes As Double

				dblTotalMinutes = System.DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes

				ioFileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION))
				If ioFileInfo.Exists Then
					dblFileSizeMB = ioFileInfo.Length / 1024.0 / 1024
				End If

				ioFileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION))
				If ioFileInfo.Exists Then
					dblXMLSizeMB = ioFileInfo.Length / 1024.0 / 1024
				End If

				strMessage = "mzXML creation time = " & dblTotalMinutes.ToString("0.00") & " minutes"

				If dblTotalMinutes > 0 Then
					strMessage &= "; Processing rate = " & (dblFileSizeMB / dblTotalMinutes / 60).ToString("0.0") & " MB/second"
				End If

				strMessage &= "; .Raw file size = " & dblFileSizeMB.ToString("0.0") & " MB"
				strMessage &= "; .mzXML file size = " & dblXMLSizeMB.ToString("0.0") & " MB"

				If dblFileSizeMB > 0 Then
					strMessage &= "; Filesize Ratio = " & (dblXMLSizeMB / dblFileSizeMB).ToString("0.00")
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception saving mzXML stats", ex)
			End Try
		End If

		Return True

	End Function

	Protected Function DefineProgramPaths() As Boolean

		' mJavaProgLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
		' Note that we need to run MSGF with a 64-bit version of Java since it prefers to use 2 or more GB of ram
		mJavaProgLoc = m_mgrParams.GetParam("JavaLoc")
		If Not System.IO.File.Exists(mJavaProgLoc) Then
			If mJavaProgLoc.Length = 0 Then mJavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
			m_message = "Cannot find Java: " & mJavaProgLoc
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		' Determine the path to the MSGFDB program (which contains the MSGF class); we also allow for the possibility of calling the legacy version of MSGF
		mMSGFProgLoc = DetermineMSGFProgramLocation(mUsingMSGFDB)

		If String.IsNullOrEmpty(mMSGFProgLoc) Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error determining MSGF program location"
			End If
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		' Determine the path to the XML Generator
		mMSXmlGeneratorExe = m_jobParams.GetParam("MSXMLGenerator")			' ReadW.exe or MSConvert.exe (code will assume ReadW.exe if an empty string)

		If String.IsNullOrEmpty(mMSXmlGeneratorExe) Then
			' Assume we're using ReadW
			mMSXmlGeneratorExe = "ReadW.exe"
		End If

		mMSXmlGeneratorAppPath = String.Empty
		If mMSXmlGeneratorExe.ToLower().Contains("readw") Then
			' ReadW
			' Note that msXmlGenerator will likely be ReAdW.exe
			mMSXmlGeneratorAppPath = MyBase.DetermineProgramLocation("ReAdW", "ReAdWProgLoc", mMSXmlGeneratorExe)

		ElseIf mMSXmlGeneratorExe.ToLower().Contains("msconvert") Then
			' MSConvert
			Dim ProteoWizardDir As String = m_mgrParams.GetParam("ProteoWizardDir")			' MSConvert.exe is stored in the ProteoWizard folder
			mMSXmlGeneratorAppPath = System.IO.Path.Combine(ProteoWizardDir, mMSXmlGeneratorExe)

		Else
			m_message = "Invalid value for MSXMLGenerator; should be 'ReadW' or 'MSConvert'"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		Return True

	End Function

	Protected Sub DeleteTemporaryfile(ByVal strFilePath As String)

		Try
			If System.IO.File.Exists(strFilePath) Then
				System.IO.File.Delete(strFilePath)
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception deleting temporary file " & strFilePath, ex)
		End Try

	End Sub

	Protected Function DetermineMSGFProgramLocation(ByRef blnUsingMSGFDB As Boolean) As String

		Dim strStepToolName As String = "MSGFDB"
		Dim strProgLocManagerParamName As String = "MSGFDbProgLoc"
		Dim strExeName As String = MSGFDB_JAR_NAME

		blnUsingMSGFDB = True

		' Note that as of 12/20/2011 we are using MSGFDB.jar to access the MSGF class
		' In order to allow the old version of MSGF to be run, we must look for parameter MSGF_Version

		' Check whether the settings file specifies that a specific version of the step tool be used
		Dim strMSGFStepToolVersion As String = m_jobParams.GetParam("MSGF_Version")

		If Not String.IsNullOrWhiteSpace(strMSGFStepToolVersion) Then

			' Specific version is defined
			' Check whether the version is one of the known versions for the old MSGF

			If IsLegacyMSGFVersion(strMSGFStepToolVersion) Then
				' Use MSGF

				strStepToolName = "MSGF"
				strProgLocManagerParamName = "MSGFLoc"
				strExeName = MSGF_JAR_NAME

				blnUsingMSGFDB = False

			Else
				' Use MSGFDB
				blnUsingMSGFDB = True
				mMSGFDBVersion = String.Copy(strMSGFStepToolVersion)
			End If

		Else
			' Use MSGFDB
			blnUsingMSGFDB = True
			mMSGFDBVersion = "Production_Release"
		End If

		Return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strMSGFStepToolVersion)

	End Function

	Public Shared Function GetPeptideHitResultType(ByVal strPeptideHitResultType As String) As clsPHRPReader.ePeptideHitResultType
		Select Case strPeptideHitResultType.ToLower
			Case "Peptide_Hit".ToLower
				Return clsPHRPReader.ePeptideHitResultType.Sequest

			Case "XT_Peptide_Hit".ToLower
				Return clsPHRPReader.ePeptideHitResultType.XTandem

			Case "IN_Peptide_Hit".ToLower
				Return clsPHRPReader.ePeptideHitResultType.Inspect

			Case "MSG_Peptide_Hit".ToLower
				Return clsPHRPReader.ePeptideHitResultType.MSGFDB

			Case Else
				Return clsPHRPReader.ePeptideHitResultType.Unknown
		End Select
	End Function

	Public Shared Function IsLegacyMSGFVersion(ByVal strStepToolVersion As String) As Boolean

		Select Case strStepToolVersion.ToLower()
			Case "v2010-11-16", "v2011-09-02", "v6393", "v6432"
				' Legacy MSGF
				Return True

			Case Else
				' Using MSGF inside MSGFDB
				Return False

		End Select

	End Function

	''' <summary>
	''' Compare intPrecursorMassErrorCount to intLinesRead
	''' If more than 10% of the results have a precursor mass error, then set blnTooManyPrecursorMassMismatches to True
	''' </summary>
	''' <param name="intLinesRead"></param>
	''' <param name="intPrecursorMassErrorCount"></param>
	''' <param name="blnTooManyPrecursorMassMismatches"></param>
	''' <remarks></remarks>
	Private Sub PostProcessMSGFCheckPrecursorMassErrorCount(ByVal intLinesRead As Integer, ByVal intPrecursorMassErrorCount As Integer, ByRef blnTooManyPrecursorMassMismatches As Boolean)

		Const MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT As Integer = 10

		Dim sngPercentDataPrecursorMassError As Single
		Dim Msg As String

		Try
			' If 10% or more of the data has a message like "N/A: precursor mass != peptide mass (3571.8857 vs 3581.9849)"
			' then set blnTooManyPrecursorMassMismatches to True

			blnTooManyPrecursorMassMismatches = False

			If intLinesRead >= 2 AndAlso intPrecursorMassErrorCount > 0 Then
				sngPercentDataPrecursorMassError = CSng(intPrecursorMassErrorCount / intLinesRead * 100)

				Msg = sngPercentDataPrecursorMassError.ToString("0.0") & "% of the data processed by MSGF has a precursor mass 10 or more Da away from the computed peptide mass"

				If sngPercentDataPrecursorMassError >= MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT Then
					Msg &= "; this likely indicates a static or dynamic mod definition is missing from the PHRP _ModSummary.txt file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
					blnTooManyPrecursorMassMismatches = True
				Else
					Msg &= "; this is below the error threshold of " & MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT & "% and thus is only a warning (note that static and dynamic mod info is loaded from the PHRP _ModSummary.txt file)"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
				End If
			End If

		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub

	''' <summary>
	''' Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
	''' Will also look for non-numeric values in the SpecProb column
	''' Examples:
	'''   N/A: unrecognizable annotation
	'''   N/A: precursor mass != peptide mass (4089.068 vs 4078.069)
	''' the new MSGF result files will guarantee that the SpecProb column has a number, 
	'''   but will have an additional column called SpecProbNotes with any notes or warnings
	''' The synopsis-based MSGF results will be extended to include any entries skipped when
	'''  creating the MSGF input file (to aid in linking up files later)
	''' </summary>
	''' <param name="strMSGFResultsFilePath">MSGF results file to examine</param>
	''' <returns>True if success; false if one or more errors</returns>
	''' <remarks></remarks>
	Protected Function PostProcessMSGFResults(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, ByVal strMSGFResultsFilePath As String) As Boolean

		Dim fiInputFile As System.IO.FileInfo
		Dim fiMSGFSynFile As System.IO.FileInfo

		Dim strMSGFSynopsisResults As String = String.Empty
		Dim Msg As String

		Dim blnSuccess As Boolean
		Dim blnFirstHitsDataPresent As Boolean = False
		Dim blnTooManyPrecursorMassMismatches As Boolean = False

		Try
			If String.IsNullOrEmpty(strMSGFResultsFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSGF Results File path is empty; unable to continue")
				Return False
			End If

			m_StatusTools.CurrentOperation = "MSGF complete; post-processing the results"

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGF complete; post-processing the results")
			End If

			fiInputFile = New System.IO.FileInfo(strMSGFResultsFilePath)

			' Define the path to write the synopsis MSGF results to
			strMSGFSynopsisResults = System.IO.Path.Combine(fiInputFile.DirectoryName, _
			 System.IO.Path.GetFileNameWithoutExtension(fiInputFile.Name) & "_PostProcess.txt")

			m_progress = PROGRESS_PCT_MSGF_POST_PROCESSING
			m_StatusTools.UpdateAndWrite(m_progress)

			blnSuccess = PostProcessMSGFResultsWork(eResultType, strMSGFResultsFilePath, strMSGFSynopsisResults, blnFirstHitsDataPresent, blnTooManyPrecursorMassMismatches)

		Catch ex As Exception
			Msg = "Error post-processing the MSGF Results file: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception post-processing the MSGF Results file")

			Return False
		End Try

		Try
			' Now replace the _MSGF.txt file with the _MSGF_PostProcess.txt file
			' For example, replace:
			'   QC_Shew_Dataset_syn_MSGF.txt
			' With the contents of:
			'   QC_Shew_Dataset_syn_MSGF_PostProcess.txt

			System.Threading.Thread.Sleep(500)

			' Delete the original file
			fiInputFile.Delete()
			System.Threading.Thread.Sleep(500)

			' Rename the _PostProcess.txt file
			fiMSGFSynFile = New System.IO.FileInfo(strMSGFSynopsisResults)

			fiMSGFSynFile.MoveTo(strMSGFResultsFilePath)

		Catch ex As Exception
			Msg = "Error replacing the original MSGF Results file with the post-processed one: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception post-processing the MSGF Results file")

			Return False
		End Try

		If blnSuccess Then
			' Summarize the results in the _syn_MSGF.txt file
			' Post the results to the database
			SummarizeMSGFResults(eResultType)
		End If

		If blnSuccess AndAlso blnFirstHitsDataPresent Then
			' Write out the First-Hits file results
			blnSuccess = mMSGFInputCreator.CreateMSGFFirstHitsFile()
		End If

		If blnTooManyPrecursorMassMismatches Then
			Return False
		Else
			Return blnSuccess
		End If

	End Function

	''' <summary>
	''' Process the data in strMSGFResultsFilePath to create strMSGFSynopsisResults
	''' </summary>
	''' <param name="eResultType"></param>
	''' <param name="strMSGFResultsFilePath"></param>
	''' <param name="strMSGFSynopsisResults"></param>
	''' <param name="blnFirstHitsDataPresent"></param>
	''' <param name="blnTooManyPrecursorMassMismatches"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function PostProcessMSGFResultsWork(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, _
	  ByVal strMSGFResultsFilePath As String, _
	  ByVal strMSGFSynopsisResults As String, _
	  ByRef blnFirstHitsDataPresent As Boolean, _
	  ByRef blnTooManyPrecursorMassMismatches As Boolean) As Boolean

		Const MAX_ERRORS_TO_LOG As Integer = 5

		Dim chSepChars() As Char = New Char() {ControlChars.Tab}

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

		Dim intLinesRead As Integer
		Dim intErrorCount As Integer
		Dim intPrecursorMassErrorCount As Integer

		Dim strOriginalPeptide As String
		Dim strScan As String
		Dim strCharge As String
		Dim strProtein As String
		Dim strPeptide As String
		Dim strResultID As String
		Dim strSpecProb As String
		Dim strDataSource As String
		Dim strNotes As String

		Dim strMSGFResultData As String
		Dim strOriginalPeptideInfo As String
		Dim strProteinNew As String = String.Empty

		Dim intResultID As Integer
		Dim intIndex As Integer
		Dim dblSpecProb As Double = 0

		Dim objSkipList As System.Collections.Generic.List(Of String)
		Dim strSkipInfo() As String

		Dim blnSkipLine As Boolean
		Dim blnHeaderLineParsed As Boolean

		'''''''''''''''''''''''''''''
		' Note: Do not put a Try/Catch block in this function
		' Allow the calling function to catch any errors
		'''''''''''''''''''''''''''''

		' Initialize the column mapping
		' Using a case-insensitive comparer
		objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

		' Define the default column mapping
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_SpectrumFile, 0)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Title, 1)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_ScanNumber, 2)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Annotation, 3)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Charge, 4)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Protein_First, 5)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Result_ID, 6)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Data_Source, 7)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_Collision_Mode, 8)
		objColumnHeaders.Add(MSGF_RESULT_COLUMN_SpecProb, 9)

		' Read the data from the MSGF Result file
		Using srMSGFResults As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strMSGFResultsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

			' Create a new file for writing the Synopsis MSGF Results
			Using swMSGFSynFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFSynopsisResults, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

				' Write out the headers to swMSGFSynFile
				mMSGFInputCreator.WriteMSGFResultsHeaders(swMSGFSynFile)

				blnHeaderLineParsed = False
				blnFirstHitsDataPresent = False

				intLinesRead = 0
				intErrorCount = 0
				intPrecursorMassErrorCount = 0

				Do While srMSGFResults.Peek >= 0
					strLineIn = srMSGFResults.ReadLine
					intLinesRead += 1
					blnSkipLine = False

					If Not String.IsNullOrEmpty(strLineIn) Then
						strSplitLine = strLineIn.Split(ControlChars.Tab)

						If Not blnHeaderLineParsed Then
							If strSplitLine(0).ToLower() = MSGF_RESULT_COLUMN_SpectrumFile.ToLower() Then
								' Parse the header line to confirm the column ordering
								clsPHRPReader.ParseColumnHeaders(strSplitLine, objColumnHeaders)
								blnSkipLine = True
							End If

							blnHeaderLineParsed = True
						End If

						If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

							strOriginalPeptide = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Title, objColumnHeaders)
							strScan = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_ScanNumber, objColumnHeaders)
							strCharge = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Charge, objColumnHeaders)
							strProtein = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Protein_First, objColumnHeaders)
							strPeptide = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Annotation, objColumnHeaders)
							strResultID = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Result_ID, objColumnHeaders)
							strSpecProb = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_SpecProb, objColumnHeaders)
							strDataSource = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Data_Source, objColumnHeaders)
							strNotes = String.Empty

							If Double.TryParse(strSpecProb, dblSpecProb) Then
								If strOriginalPeptide <> strPeptide Then
									strNotes = String.Copy(strPeptide)
								End If

								' Update strSpecProb to reduce the number of significant figures
								strSpecProb = dblSpecProb.ToString("0.000000E+00")
							Else

								' The specProb column does not contain a number
								intErrorCount += 1

								If intErrorCount <= MAX_ERRORS_TO_LOG Then
									' Log the first 5 instances to the log file as warnings

									If strOriginalPeptide <> strPeptide Then
										strOriginalPeptideInfo = ", original peptide sequence " & strOriginalPeptide
									Else
										strOriginalPeptideInfo = String.Empty
									End If

									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF SpecProb is not numeric on line " & intLinesRead & " in the result file: " & strSpecProb & " (parent peptide " & strPeptide & ", Scan " & strScan & ", Result_ID " & strResultID & strOriginalPeptideInfo & ")")
								End If

								If strSpecProb.Contains("precursor mass") Then
									intPrecursorMassErrorCount += 1
								End If

								If strOriginalPeptide <> strPeptide Then
									strNotes = strPeptide & "; " & strSpecProb
								Else
									strNotes = String.Copy(strSpecProb)
								End If

								' Change the spectrum probability to 1
								strSpecProb = "1"
							End If

							strMSGFResultData = strScan & ControlChars.Tab & _
							  strCharge & ControlChars.Tab & _
							  strProtein & ControlChars.Tab & _
							  strOriginalPeptide & ControlChars.Tab & _
							  strSpecProb & ControlChars.Tab & _
							  strNotes

							' Add this result to the cached string dictionary
							mMSGFInputCreator.AddUpdateMSGFResult(strScan, strCharge, strOriginalPeptide, strMSGFResultData)


							If strDataSource = MSGF_PHRP_DATA_SOURCE_FHT Then
								' First-hits file
								blnFirstHitsDataPresent = True

							Else
								' Synopsis file

								' Add this entry to the MSGF synopsis results
								' Note that strOriginalPeptide has the original peptide sequence
								swMSGFSynFile.WriteLine(strResultID & ControlChars.Tab & strMSGFResultData)

								' See if any entries were skipped when reading the synopsis file used to create the MSGF input file
								' If they were, add them to the validated MSGF file (to aid in linking up files later)

								If Integer.TryParse(strResultID, intResultID) Then
									objSkipList = mMSGFInputCreator.GetSkippedInfoByResultId(intResultID)

									For intIndex = 0 To objSkipList.Count - 1

										' Split the entry on the tab character
										' The item left of the tab is the skipped result id
										' the item right of the tab is the protein corresponding to the skipped result id

										strSkipInfo = objSkipList(intIndex).Split(chSepChars, 2)

										swMSGFSynFile.WriteLine( _
										  strSkipInfo(0) & ControlChars.Tab & _
										  strScan & ControlChars.Tab & _
										  strCharge & ControlChars.Tab & _
										  strSkipInfo(1) & ControlChars.Tab & _
										  strOriginalPeptide & ControlChars.Tab & _
										  strSpecProb & ControlChars.Tab & _
										  strNotes)

									Next
								End If
							End If

						End If
					End If

				Loop

			End Using

		End Using

		If intErrorCount > 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF SpecProb was not numeric for " & intErrorCount & " entries in the MSGF result file")
		End If

		' Check whether more than 10% of the results have a precursor mass error
		PostProcessMSGFCheckPrecursorMassErrorCount(intLinesRead, intPrecursorMassErrorCount, blnTooManyPrecursorMassMismatches)

		' If we get here, return True
		Return True

	End Function

	Protected Function ProcessFileWithMSGF(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, _
	 ByVal intMSGFInputFileLineCount As Integer, _
	 ByVal strMSGFInputFilePath As String, _
	 ByVal strMSGFResultsFilePath As String) As Boolean

		Dim blnSuccess As Boolean = False

		If eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
			' Input file may contain a mix of scan types (CID, ETD, and/or HCD)
			' If this is the case, then need to call MSGF twice: first for the CID and HCD spectra, then again for the ETD spectra
			blnSuccess = RunMSGFonMSGFDB(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath)

		Else
			' Run MSGF
			blnSuccess = RunMSGF(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath)
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Looks for file strFileNameToFind in the transfer folder for this job
	''' If found, copies the file to the work directory
	''' </summary>
	''' <param name="strFileNameToFind"></param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Protected Function RetrievePreGeneratedDataFile(ByVal strFileNameToFind As String) As Boolean

		Dim strTransferFolderPath As String
		Dim strInputFolderName As String
		Dim strFolderToCheck As String = "??"
		Dim strFilePathSource As String
		Dim strFilePathTarget As String

		Try
			strTransferFolderPath = m_jobParams.GetParam("transferFolderPath")
			strInputFolderName = m_jobParams.GetParam("inputFolderName")

			strFolderToCheck = System.IO.Path.Combine(System.IO.Path.Combine(strTransferFolderPath, m_Dataset), strInputFolderName)

			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for folder " & strFolderToCheck)
			End If

			' Look for strFileNameToFind in strFolderToCheck
			If System.IO.Directory.Exists(strFolderToCheck) Then
				strFilePathSource = System.IO.Path.Combine(strFolderToCheck, strFileNameToFind)

				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for file " & strFilePathSource)
				End If

				If System.IO.File.Exists(strFilePathSource) Then
					strFilePathTarget = System.IO.Path.Combine(m_WorkDir, strFileNameToFind)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file " & strFilePathSource & " to " & strFilePathTarget)

					System.IO.File.Copy(strFilePathSource, strFilePathTarget, True)

					' File found and successfully copied; return true
					Return True
				End If
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception finding file " & strFileNameToFind & " in folder " & strFolderToCheck, ex)
			Return False
		End Try

		' File not found
		Return False

	End Function

	Protected Function RunMSGFonMSGFDB(ByVal intMSGFInputFileLineCount As Integer, ByVal strMSGFInputFilePath As String, ByVal strMSGFResultsFilePath As String) As Boolean

		Dim strLineIn As String
		Dim intLinesRead As Integer
		Dim strSplitLine() As String

		Dim lstCIDData As System.Collections.Generic.List(Of String)
		Dim lstETDData As System.Collections.Generic.List(Of String)
		Dim intCollisionModeColIndex As Integer = -1

		Try
			lstCIDData = New System.Collections.Generic.List(Of String)
			lstETDData = New System.Collections.Generic.List(Of String)

			Using srSourceFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strMSGFInputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				intLinesRead = 0
				Do While srSourceFile.Peek > -1
					strLineIn = srSourceFile.ReadLine()

					If Not String.IsNullOrEmpty(strLineIn) Then
						intLinesRead += 1
						strSplitLine = strLineIn.Split(ControlChars.Tab)

						If intLinesRead = 1 Then
							' Cache the header line
							lstCIDData.Add(strLineIn)
							lstETDData.Add(strLineIn)

							' Confirm the column index of the Collision_Mode column
							For intIndex As Integer = 0 To strSplitLine.Length - 1
								If strSplitLine(intIndex).ToLower() = MSGF_RESULT_COLUMN_Collision_Mode.ToLower() Then
									intCollisionModeColIndex = intIndex
								End If
							Next

							If intCollisionModeColIndex < 0 Then
								' Collision_Mode column not found; this is unexpected
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Collision_Mode column not found in the MSGF input file for MSGFDB data; unable to continue")
								srSourceFile.Close()
								Return False
							End If

						Else
							' Read the collision mode

							If strSplitLine.Length > intCollisionModeColIndex Then
								If strSplitLine(intCollisionModeColIndex).ToUpper() = "ETD" Then
									lstETDData.Add(strLineIn)
								Else
									lstCIDData.Add(strLineIn)
								End If
							Else
								lstCIDData.Add(strLineIn)
							End If
						End If

					End If
				Loop

			End Using

			mProcessingMSGFDBCollisionModeData = False

			If lstCIDData.Count <= 1 And lstETDData.Count > 1 Then
				' Only ETD data is present
				mETDMode = True
				Return RunMSGF(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath)

			ElseIf lstCIDData.Count > 1 And lstETDData.Count > 1 Then
				' Mix of both CID and ETD data found

				Dim blnSuccess As Boolean

				mProcessingMSGFDBCollisionModeData = True

				' Make sure the final results file does not exist
				If System.IO.File.Exists(strMSGFResultsFilePath) Then
					System.IO.File.Delete(strMSGFResultsFilePath)
				End If

				' Process the CID data
				mETDMode = False
				mCollisionModeIteration = 1
				blnSuccess = RunMSGFonMSGFDBCachedData(lstCIDData, strMSGFInputFilePath, strMSGFResultsFilePath, "CID")
				If Not blnSuccess Then Return False

				' Process the ETD data
				mETDMode = True
				mCollisionModeIteration = 2
				blnSuccess = RunMSGFonMSGFDBCachedData(lstETDData, strMSGFInputFilePath, strMSGFResultsFilePath, "ETD")
				If Not blnSuccess Then Return False

				Return True
			Else

				' Only CID or HCD data is present (or no data is present)
				mETDMode = False
				Return RunMSGF(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath)

			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in RunMSGFonMSGFDB", ex)
			Return False
		End Try

		Return True
	End Function

	Protected Function RunMSGFonMSGFDBCachedData( _
	   ByRef lstData As System.Collections.Generic.List(Of String), _
	   ByVal strMSGFInputFilePath As String, _
	   ByVal strMSGFResultsFilePathFinal As String, _
	   ByVal strCollisionMode As String) As Boolean

		Dim strInputFileTempPath As String
		Dim strResultFileTempPath As String

		Dim blnSuccess As Boolean

		Try

			strInputFileTempPath = AddFileNameSuffix(strMSGFInputFilePath, strCollisionMode)
			strResultFileTempPath = AddFileNameSuffix(strMSGFResultsFilePathFinal, strCollisionMode)

			Using swInputFileTemp As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strInputFileTempPath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
				For Each strData As String In lstData
					swInputFileTemp.WriteLine(strData)
				Next
			End Using

			blnSuccess = RunMSGF(lstData.Count - 1, strInputFileTempPath, strResultFileTempPath)

			If Not blnSuccess Then
				Return False
			End If

			System.Threading.Thread.Sleep(500)

			' Append the results of strResultFileTempPath to strMSGFResultsFilePath
			If Not System.IO.File.Exists(strMSGFResultsFilePathFinal) Then
				System.IO.File.Move(strResultFileTempPath, strMSGFResultsFilePathFinal)
			Else
				Using srTempResults As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strResultFileTempPath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
					Using swFinalResults As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFResultsFilePathFinal, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))

						' Read and skip the first line of srTempResults (it's a header)
						srTempResults.ReadLine()

						' Append the remaining lines to swFinalResults
						While srTempResults.Peek > -1
							swFinalResults.WriteLine(srTempResults.ReadLine)
						End While

					End Using
				End Using

			End If

			System.Threading.Thread.Sleep(500)

			If Not mKeepMSGFInputFiles Then

				' Delete the temporary files
				DeleteTemporaryfile(strInputFileTempPath)
				DeleteTemporaryfile(strResultFileTempPath)

			End If


		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in RunMSGFonMSGFDBCachedData", ex)
			Return False
		End Try

		Return True

	End Function

	Protected Function RunMSGF(ByVal intMSGFInputFileLineCount As Integer, ByVal strMSGFInputFilePath As String, ByVal strMSGFResultsFilePath As String) As Boolean

		Dim intMSGFEntriesPerSegment As Integer = 0
		Dim blnSuccess As Boolean
		Dim blnUseSegments As Boolean = False
		Dim strSegmentUsageMessage As String = String.Empty

		intMSGFEntriesPerSegment = m_JobParams.GetJobParameter("MSGFEntriesPerSegment", MSGF_SEGMENT_ENTRY_COUNT)
		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFInputFileLineCount = " & intMSGFInputFileLineCount & "; MSGFEntriesPerSegment = " & intMSGFEntriesPerSegment)
		End If

		If intMSGFEntriesPerSegment <= 1 Then
			blnUseSegments = False
			strSegmentUsageMessage = "Not using MSGF segments since MSGFEntriesPerSegment is <= 1"

		ElseIf intMSGFInputFileLineCount <= intMSGFEntriesPerSegment * MSGF_SEGMENT_OVERFLOW_MARGIN Then
			blnUseSegments = False
			strSegmentUsageMessage = "Not using MSGF segments since MSGFInputFileLineCount is <= " & intMSGFEntriesPerSegment & " * " & CInt(MSGF_SEGMENT_OVERFLOW_MARGIN * 100).ToString() & "%"

		Else
			blnUseSegments = True
			strSegmentUsageMessage = "Using MSGF segments"
		End If

		mMSGFLineCountPreviousSegments = 0
		mMSGFInputFileLineCount = intMSGFInputFileLineCount
		m_progress = PROGRESS_PCT_MSGF_START


		If Not blnUseSegments Then
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strSegmentUsageMessage)
			End If

			' Do not use segments
			blnSuccess = RunMSGFWork(strMSGFInputFilePath, strMSGFResultsFilePath)

		Else

			Dim lstSegmentFileInfo As New System.Collections.Generic.List(Of udtSegmentFileInfoType)
			Dim udtSegmentFile As udtSegmentFileInfoType
			Dim lstResultFiles As System.Collections.Generic.List(Of String)
			lstResultFiles = New System.Collections.Generic.List(Of String)

			' Split strMSGFInputFilePath into chunks with intMSGFEntriesPerSegment each
			blnSuccess = SplitMSGFInputFile(intMSGFInputFileLineCount, strMSGFInputFilePath, intMSGFEntriesPerSegment, lstSegmentFileInfo)

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strSegmentUsageMessage & "; segment count = " & lstSegmentFileInfo.Count)
			End If

			If blnSuccess Then

				' Call MSGF for each segment
				For Each udtSegmentFile In lstSegmentFileInfo
					Dim strResultFile As String
					strResultFile = AddFileNameSuffix(strMSGFResultsFilePath, udtSegmentFile.Segment)

					blnSuccess = RunMSGFWork(udtSegmentFile.FilePath, strResultFile)

					If Not blnSuccess Then Exit For

					lstResultFiles.Add(strResultFile)
					mMSGFLineCountPreviousSegments += udtSegmentFile.Entries
				Next
			End If

			If blnSuccess Then
				' Combine the results
				blnSuccess = CombineMSGFResultFiles(strMSGFResultsFilePath, lstResultFiles)
			End If


			If blnSuccess Then
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting MSGF segment files")
				End If

				' Delete the segment files
				For Each udtSegmentFile In lstSegmentFileInfo
					DeleteTemporaryfile(udtSegmentFile.FilePath)
				Next

				' Delete the result files
				For Each strResultFile As String In lstResultFiles
					DeleteTemporaryfile(strResultFile)
				Next
			End If
		End If

		Try
			' Delete the Console_Output.txt file if it is empty
			Dim fiConsoleOutputFile As System.IO.FileInfo
			fiConsoleOutputFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT))
			If fiConsoleOutputFile.Exists AndAlso fiConsoleOutputFile.Length = 0 Then
				fiConsoleOutputFile.Delete()
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete the " & MSGF_CONSOLE_OUTPUT & " file", ex)
		End Try

		Return blnSuccess

	End Function

	Protected Function RunMSGFWork(ByVal strInputFilePath As String, ByVal strResultsFilePath As String) As Boolean

		Dim CmdStr As String
		Dim intJavaMemorySize As Integer

		If String.IsNullOrEmpty(strInputFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "strInputFilePath has not been defined; unable to continue")
			Return False
		End If

		If String.IsNullOrEmpty(strResultsFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "strResultsFilePath has not been defined; unable to continue")
			Return False
		End If

		mMSGFRunner = New clsRunDosProgram(m_WorkDir)

		' If an MSGF analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
		' Customize this on a per-job basis using the MSGFJavaMemorySize setting in the settings file 
		' (job 611216 succeeded with a value of 5000)
		intJavaMemorySize = m_JobParams.GetJobParameter("MSGFJavaMemorySize", 2000)
		If intJavaMemorySize < 512 Then intJavaMemorySize = 512

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSGF on " & System.IO.Path.GetFileName(strInputFilePath))
		End If

		mCurrentMSGFResultsFilePath = String.Copy(strResultsFilePath)

		m_StatusTools.CurrentOperation = "Running MSGF"
		m_StatusTools.UpdateAndWrite(m_progress)

		CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M "

		If mUsingMSGFDB Then
			CmdStr &= "-cp " & PossiblyQuotePath(mMSGFProgLoc) & " ui.MSGF"
		Else
			CmdStr &= "-jar " & PossiblyQuotePath(mMSGFProgLoc)
		End If

		CmdStr &= " -i " & PossiblyQuotePath(strInputFilePath)			 ' Input file
		CmdStr &= " -d " & PossiblyQuotePath(m_WorkDir)					 ' Folder containing .mzXML file
		CmdStr &= " -o " & PossiblyQuotePath(strResultsFilePath)		 ' Output file

		' MSGF v6432 and earlier use -m 0 for CID and -m 1 for ETD
		' MSGFDB v7097 and later use: 
		'   -m 0 means as written in the spectrum or CID if no info
		'   -m 1 means CID
		'   -m 2 means ETD
		'   -m 3 means HCD

		Dim intMSGFDBVersion As Integer = Integer.MaxValue

		If mUsingMSGFDB Then
			If Not String.IsNullOrEmpty(mMSGFDBVersion) AndAlso mMSGFDBVersion.StartsWith("v") Then
				If Integer.TryParse(mMSGFDBVersion.Substring(1), intMSGFDBVersion) Then
					' Using a specific version of MSGFDB
					' intMSGFDBVersion should now be something like 6434, 6841, 6964, 7097 etc.
				Else
					' Unable to parse out an integer from mMSGFDBVersion
					intMSGFDBVersion = Integer.MaxValue
				End If
			End If
		End If

		If mUsingMSGFDB AndAlso intMSGFDBVersion >= 7097 Then
			' Always use -m 0 (assuming we're sending an mzXML file to MSGFDB)
			CmdStr &= " -m 0"	' as-written in the input file
		Else
			If mETDMode Then
				CmdStr &= " -m 1"	' ETD fragmentation
			Else
				CmdStr &= " -m 0"	' CID fragmentation
			End If
		End If


		CmdStr &= " -e 1"		' Enzyme is Trypsin; other supported enzymes are 2: Chymotrypsin, 3: Lys-C, 4: Lys-N, 5: Glu-C, 6: Arg-C, 7: Asp-N, and 8: aLP
		CmdStr &= " -fixMod 0"	' No fixed mods on cysteine
		CmdStr &= " -x 0"		' Write out all matches for each spectrum
		CmdStr &= " -p 1"		' SpecProbThreshold threshold of 1, i.e., do not filter results by the computed SpecProb value

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc & " " & CmdStr)

		With mMSGFRunner
			.CreateNoWindow = False
			.CacheStandardOutput = False
			.EchoOutputToConsole = False

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT)
		End With

		Dim blnSuccess As Boolean
		blnSuccess = mMSGFRunner.RunProgram(mJavaProgLoc, CmdStr, "MSGF", True)

		If Not mToolVersionWritten Then
			If String.IsNullOrWhiteSpace(mMSGFVersion) Then
				Dim fiConsoleOutputfile As New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT))
				If fiConsoleOutputfile.Length = 0 Then
					' File is 0-bytes; delete it
					DeleteTemporaryfile(fiConsoleOutputfile.FullName)
				Else
					ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT))
				End If
			End If
			mToolVersionWritten = StoreToolVersionInfo()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running MSGF, job " & m_JobNum)
		End If

		Return blnSuccess

	End Function

	Protected Function CombineMSGFResultFiles(ByVal strMSGFOutputFilePath As String, _
	   ByRef lstResultFiles As System.Collections.Generic.List(Of String)) As Boolean

		Try

			Dim strLineIn As String
			Dim intLinesRead As Integer
			Dim blnHeaderWritten As Boolean

			' Create the output file
			Using swOutFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

				' Step through the input files and append the results
				blnHeaderWritten = False
				For Each strResultFile As String In lstResultFiles
					Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strResultFile, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
						intLinesRead = 0
						Do While srInFile.Peek >= 0
							strLineIn = srInFile.ReadLine()
							intLinesRead += 1

							If Not blnHeaderWritten Then
								blnHeaderWritten = True
								swOutFile.WriteLine(strLineIn)
							Else
								If intLinesRead > 1 Then
									swOutFile.WriteLine(strLineIn)
								End If
							End If

						Loop
					End Using

				Next

			End Using

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception combining MSGF result files", ex)
			Return False
		End Try

		Return True

	End Function


	''' <summary>
	''' Parse the MSGF console output file to determine the MSGF version
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		Try

			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If

			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim intLinesRead As Integer

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			intLinesRead = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					If intLinesRead = 1 Then
						' The first line is the MSGF version

						If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mMSGFVersion) Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGF version: " & strLineIn)
						End If

						mMSGFVersion = String.Copy(strLineIn)

					Else
						If strLineIn.ToLower.Contains("error") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running MSGF:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
						End If
					End If
				End If
			Loop

			srInFile.Close()

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub


	Protected Function SplitMSGFInputFile(ByVal intMSGFinputFileLineCount As Integer, _
	  ByVal strMSGFInputFilePath As String, _
	  ByVal intMSGFEntriesPerSegment As Integer, _
	  ByRef lstSegmentFileInfo As System.Collections.Generic.List(Of udtSegmentFileInfoType)) As Boolean

		Dim intLinesRead As Integer = 0
		Dim strLineIn As String
		Dim strHeaderLine As String = String.Empty

		Dim intLineCountAllSegments As Integer = 0
		Dim udtThisSegment As udtSegmentFileInfoType

		Try
			lstSegmentFileInfo.Clear()
			If intMSGFEntriesPerSegment < 100 Then intMSGFEntriesPerSegment = 100

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strMSGFInputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				Dim swOutFile As System.IO.StreamWriter = Nothing

				udtThisSegment.FilePath = String.Empty
				udtThisSegment.Entries = 0
				udtThisSegment.Segment = 0

				Do While srInFile.Peek >= 0
					strLineIn = srInFile.ReadLine()
					intLinesRead += 1

					If intLinesRead = 1 Then
						' This is the header line; cache it so that we can write it out to the top of each input file
						strHeaderLine = String.Copy(strLineIn)
					End If

					If udtThisSegment.Segment = 0 OrElse udtThisSegment.Entries >= intMSGFEntriesPerSegment Then
						' Need to create a new segment
						' However, if the number of lines remaining to be written is less than 5% of intMSGFEntriesPerSegment then keep writing to this segment

						Dim intLineCountRemaining As Integer
						intLineCountRemaining = intMSGFinputFileLineCount - intLineCountAllSegments

						If udtThisSegment.Segment = 0 OrElse intLineCountRemaining > intMSGFEntriesPerSegment * MSGF_SEGMENT_OVERFLOW_MARGIN Then

							If udtThisSegment.Segment > 0 Then
								' Close the current segment
								swOutFile.Close()
								lstSegmentFileInfo.Add(udtThisSegment)
							End If

							' Initialize a new segment
							udtThisSegment.Segment += 1
							udtThisSegment.Entries = 0
							udtThisSegment.FilePath = AddFileNameSuffix(strMSGFInputFilePath, udtThisSegment.Segment)

							swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(udtThisSegment.FilePath, System.IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

							' Write the header line to the new segment
							swOutFile.WriteLine(strHeaderLine)
						End If
					End If

					If intLinesRead > 1 Then
						swOutFile.WriteLine(strLineIn)
						udtThisSegment.Entries += 1
						intLineCountAllSegments += 1
					End If
				Loop

				' Close the the output files
				swOutFile.Close()
				lstSegmentFileInfo.Add(udtThisSegment)

			End Using


		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception splitting MSGF input file", ex)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mMSGFVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(mMSGFProgLoc))

		ioToolFiles.Add(New System.IO.FileInfo(mMSXmlGeneratorAppPath))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex)
			Return False
		End Try

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfoMSGFDBResults() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of AnalysisManagerMSGFPlugin
		Try
			Dim oAssemblyName As System.Reflection.AssemblyName
			oAssemblyName = System.Reflection.Assembly.Load("AnalysisManagerMSGFPlugin").GetName

			Dim strNameAndVersion As String
			strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
			strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for AnalysisManagerMSGFPlugin: " & ex.Message)
			Return False
		End Try

		' Store the path to MSGFDB.jar in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(mMSGFProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	Protected Function SummarizeMSGFResults(ByVal eResultType As clsPHRPReader.ePeptideHitResultType) As Boolean

		Dim objSummarizer As clsMSGFResultsSummarizer
		Dim strConnectionString As String
		Dim intJobNumber As Integer = 0
		Dim blnPostResultsToDB As Boolean

		Dim blnSuccess As Boolean
		Dim Msg As String

		Try

			strConnectionString = m_mgrParams.GetParam("connectionstring")
			If Integer.TryParse(m_JobNum, intJobNumber) Then
				blnPostResultsToDB = True
			Else
				blnPostResultsToDB = False
				Msg = "Job number is not numeric: " & m_JobNum & "; will not be able to post PSM results to the database"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			End If

			objSummarizer = New AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer(eResultType, m_Dataset, intJobNumber, m_WorkDir, strConnectionString)
			objSummarizer.MSGFThreshold = clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD

			objSummarizer.PostJobPSMResultsToDB = blnPostResultsToDB
			objSummarizer.SaveResultsToTextFile = False

			blnSuccess = objSummarizer.ProcessMSGFResults()

			If Not blnSuccess Then
				Msg = "Error calling ProcessMSGFResults"
				If objSummarizer.ErrorMessage.Length > 0 Then
					Msg &= ": " & objSummarizer.ErrorMessage
				End If

				Msg &= "; input file name: " & objSummarizer.MSGFSynopsisFileName

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception summarizing the MSGF results: " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Sub UpdateMSGFProgress(ByVal strMSGFResultsFilePath As String)

		Static intErrorCount As Integer = 0

		Dim intLineCount As Integer
		Dim dblFraction As Double

		Try

			If mMSGFInputFileLineCount <= 0 Then Exit Sub
			If Not System.IO.File.Exists(strMSGFResultsFilePath) Then Exit Sub

			' Read the data from the results file
			Using srMSGFResultsFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strMSGFResultsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
				intLineCount = 0

				Do While srMSGFResultsFile.Peek > -1
					srMSGFResultsFile.ReadLine()
					intLineCount += 1
				Loop

			End Using

			' Update the overall progress
			dblFraction = (intLineCount + mMSGFLineCountPreviousSegments) / mMSGFInputFileLineCount

			If mProcessingMSGFDBCollisionModeData Then
				' Running MSGF twice; first for CID spectra and then for ETD spectra
				' Divide the progress by 2, then add 0.5 if we're on the second iteration

				dblFraction = dblFraction / 2.0
				If mCollisionModeIteration > 1 Then
					dblFraction = dblFraction + 0.5
				End If
			End If

			m_progress = CSng(PROGRESS_PCT_MSGF_START + (PROGRESS_PCT_MSGF_COMPLETE - PROGRESS_PCT_MSGF_START) * dblFraction)
			m_StatusTools.UpdateAndWrite(m_progress)

		Catch ex As Exception
			' Log errors the first 3 times they occur
			intErrorCount += 1
			If intErrorCount <= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error counting the number of lines in the MSGF results file, " & strMSGFResultsFilePath, ex)
			End If
		End Try
	End Sub

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for MSXmlGenReadW.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub MSXmlGenReadW_LoopWaiting() Handles mMSXmlGen.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			m_progress = PROGRESS_PCT_MSXML_GEN_RUNNING
			m_StatusTools.UpdateAndWrite(m_progress)
		End If
	End Sub

	''' <summary>
	''' Event handler for mMSXmlGen.ProgRunnerStarting event
	''' </summary>
	''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
	''' <remarks></remarks>
	Private Sub mMSXmlGenReadW_ProgRunnerStarting(ByVal CommandLine As String) Handles mMSXmlGen.ProgRunnerStarting
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
	End Sub

	''' <summary>
	''' Event handler for Error Events reported by the MSGF Input Creator
	''' </summary>
	''' <param name="strErrorMessage"></param>
	''' <remarks></remarks>
	Private Sub mMSGFInputCreator_ErrorEvent(ByVal strErrorMessage As String) Handles mMSGFInputCreator.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reported by MSGFInputCreator; " & strErrorMessage)
	End Sub

	''' <summary>
	''' Event handler that fires while MSGF is processing
	''' </summary>
	''' <remarks></remarks>
	Private Sub mMSGFRunner_LoopWaiting() Handles mMSGFRunner.LoopWaiting
		Static dtLastUpdateTime As System.DateTime = System.DateTime.UtcNow
		Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

		If System.DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
			' Update the MSGF progress by counting the number of lines in the _MSGF.txt file
			UpdateMSGFProgress(mCurrentMSGFResultsFilePath)

			dtLastUpdateTime = System.DateTime.UtcNow
		End If

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT))
			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

		End If

	End Sub
#End Region

End Class
